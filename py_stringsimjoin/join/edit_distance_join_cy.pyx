# edit distance join
from math import floor

from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
import pandas as pd

from py_stringsimjoin.utils.generic_helper import convert_dataframe_to_array, \
    find_output_attribute_indices, get_attrs_to_project, \
    get_num_processes_to_launch, get_output_header_from_tables, \
    get_output_row_from_tables, remove_non_ascii, remove_redundant_attrs
from py_stringsimjoin.utils.missing_value_handler import \
    get_pairs_with_missing_value
from py_stringsimjoin.utils.token_ordering import \
    gen_token_ordering_for_tables, order_using_token_ordering
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_attr_type, validate_comp_op_for_sim_measure, validate_key_attr, \
    validate_input_table, validate_threshold, \
    validate_tokenizer_for_sim_measure, validate_output_attrs

# Cython imports
from cython.parallel import prange                                              

from libcpp.vector cimport vector                                               
from libcpp.set cimport set as oset                                             
from libcpp.string cimport string                                               
from libcpp cimport bool                                                        
from libcpp.map cimport map as omap                                             
from libcpp.pair cimport pair     

from py_stringsimjoin.similarity_measure.edit_distance cimport edit_distance
from py_stringsimjoin.index.cython_inverted_index cimport CythonInvertedIndex             

def edit_distance_join(ltable, rtable,
                       l_key_attr, r_key_attr,
                       l_join_attr, r_join_attr,
                       double threshold, 
                       comp_op='<=',
                       allow_missing=False,
                       l_out_attrs=None, r_out_attrs=None,
                       l_out_prefix='l_', r_out_prefix='r_',
                       out_sim_score=True, 
                       int n_jobs=1, 
                       show_progress=True,
                       tokenizer=QgramTokenizer(qval=2)):
    """Join two tables using edit distance measure.

    Finds tuple pairs from left table and right table such that the edit 
    distance between the join attributes satisfies the condition on input 
    threshold. For example, if the comparison operator is '<=', finds tuple     
    pairs whose edit distance between the strings that are the values of    
    the join attributes is less than or equal to the input threshold, as     
    specified in "threshold". 

    Note:
        Currently, this method only computes an approximate join result. This is
        because, to perform the join we transform an edit distance measure 
        between strings into an overlap measure between qgrams of the strings. 
        Hence, we need at least one qgram to be in common between two input 
        strings, to appear in the join output. For smaller strings, where all 
        qgrams of the strings differ, we cannot process them.
 
        This method implements a simplified version of the algorithm proposed in
        `Ed-Join: An Efficient Algorithm for Similarity Joins With Edit Distance
        Constraints (Chuan Xiao, Wei Wang and Xuemin Lin), VLDB 08
        <http://www.vldb.org/pvldb/1/1453957.pdf>`_. 
        
    Args:
        ltable (DataFrame): left input table.

        rtable (DataFrame): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table.

        r_join_attr (string): join attribute in right table.

        threshold (float): edit distance threshold to be satisfied.        
                                                                                
        comp_op (string): comparison operator. Supported values are '<=', '<'   
            and '=' (defaults to '<=').                                         
                                                                                
        allow_missing (boolean): flag to indicate whether tuple pairs with      
            missing value in at least one of the join attributes should be      
            included in the output (defaults to False). If this flag is set to
            True, a tuple in ltable with missing value in the join attribute 
            will be matched with every tuple in rtable and vice versa. 
                                                                                
        l_out_attrs (list): list of attribute names from the left table to be   
            included in the output table (defaults to None).                    
                                                                                
        r_out_attrs (list): list of attribute names from the right table to be  
            included in the output table (defaults to None).                    
                                                                                
        l_out_prefix (string): prefix to be used for the attribute names coming 
            from the left table, in the output table (defaults to 'l\_').       
                                                                                
        r_out_prefix (string): prefix to be used for the attribute names coming 
            from the right table, in the output table (defaults to 'r\_').      
                                                                                
        out_sim_score (boolean): flag to indicate whether the edit distance 
            score should be included in the output table (defaults to True). 
            Setting this flag to True will add a column named '_sim_score' in 
            the output table. This column will contain the edit distance scores 
            for the tuple pairs in the output.                                          

        n_jobs (int): number of parallel jobs to use for the computation        
            (defaults to 1). If -1 is given, all CPUs are used. If 1 is given,  
            no parallel computing code is used at all, which is useful for      
            debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are used      
            (where n_cpus is the total number of CPUs in the machine). Thus for 
            n_jobs = -2, all CPUs but one are used. If (n_cpus + 1 + n_jobs)    
            becomes less than 1, then no parallel computing code will be used   
            (i.e., equivalent to the default).                                                                                 
                                                                                
        show_progress (boolean): flag to indicate whether task progress should  
            be displayed to the user (defaults to True).                        

        tokenizer (Tokenizer): tokenizer to be used to tokenize the join 
            attributes during filtering, when edit distance measure is          
            transformed into an overlap measure. This must be a q-gram tokenizer
            (defaults to 2-gram tokenizer).
                                                                                
    Returns:                                                                    
        An output table containing tuple pairs that satisfy the join            
        condition (DataFrame).  
    """

    # check if the input tables are dataframes
    validate_input_table(ltable, 'left table')
    validate_input_table(rtable, 'right table')

    # check if the key attributes and join attributes exist
    validate_attr(l_key_attr, ltable.columns,
                  'key attribute', 'left table')
    validate_attr(r_key_attr, rtable.columns,
                  'key attribute', 'right table')
    validate_attr(l_join_attr, ltable.columns,
                  'join attribute', 'left table')
    validate_attr(r_join_attr, rtable.columns,
                  'join attribute', 'right table')

    # check if the join attributes are not of numeric type                      
    validate_attr_type(l_join_attr, ltable[l_join_attr].dtype,                  
                       'join attribute', 'left table')                          
    validate_attr_type(r_join_attr, rtable[r_join_attr].dtype,                  
                       'join attribute', 'right table')

    # check if the input tokenizer is valid for edit distance measure. Only
    # qgram tokenizer can be used for edit distance.
    validate_tokenizer_for_sim_measure(tokenizer, 'EDIT_DISTANCE')

    # check if the input threshold is valid
    validate_threshold(threshold, 'EDIT_DISTANCE')

    # check if the comparison operator is valid
    validate_comp_op_for_sim_measure(comp_op, 'EDIT_DISTANCE')

    # check if the output attributes exist
    validate_output_attrs(l_out_attrs, ltable.columns,
                          r_out_attrs, rtable.columns)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

#                                     vector[vector[int]]& ltokens, 
#                                     vector[vector[int]]& rtokens,
#                                     int qval,
#                                     double threshold,
#                                     vector[string]& lstrings,
#                                     vector[string]& rstrings, int n_jobs):                                           
    # convert threshold to integer (incase if it is float)
    threshold = int(floor(threshold))

    # set return_set flag of tokenizer to be False, in case it is set to True
    revert_tokenizer_return_set_flag = False
    if tokenizer.get_return_set():
        tokenizer.set_return_set(False)
        revert_tokenizer_return_set_flag = True

    # remove redundant attrs from output attrs.
    l_out_attrs = remove_redundant_attrs(l_out_attrs, l_key_attr)
    r_out_attrs = remove_redundant_attrs(r_out_attrs, r_key_attr)

    # get attributes to project.  
    l_proj_attrs = get_attrs_to_project(l_out_attrs, l_key_attr, l_join_attr)
    r_proj_attrs = get_attrs_to_project(r_out_attrs, r_key_attr, r_join_attr)

    # Do a projection on the input dataframes to keep only the required         
    # attributes. Then, remove rows with missing value in join attribute from   
    # the input dataframes. Then, convert the resulting dataframes into ndarray.    
    ltable_array = convert_dataframe_to_array(ltable, l_proj_attrs, l_join_attr)
    rtable_array = convert_dataframe_to_array(rtable, r_proj_attrs, r_join_attr)

    # computes the actual number of jobs to launch.
    n_jobs = min(get_num_processes_to_launch(n_jobs), len(rtable_array))

    # find column indices of key attr, join attr and output attrs in ltable
    l_key_attr_index = l_proj_attrs.index(l_key_attr)
    l_join_attr_index = l_proj_attrs.index(l_join_attr)
    l_out_attrs_indices = find_output_attribute_indices(l_proj_attrs, l_out_attrs)

    # find column indices of key attr, join attr and output attrs in rtable
    r_key_attr_index = r_proj_attrs.index(r_key_attr)
    r_join_attr_index = r_proj_attrs.index(r_join_attr)
    r_out_attrs_indices = find_output_attribute_indices(r_proj_attrs, r_out_attrs)

    sim_measure_type = 'EDIT_DISTANCE'
    # generate token ordering using tokens in l_join_attr
    # and r_join_attr
    token_ordering = gen_token_ordering_for_tables(
                         [ltable_array, rtable_array],
                         [l_join_attr_index, r_join_attr_index],
                         tokenizer, sim_measure_type)

    cdef vector[vector[int]] ltokens
    cdef vector[string] lstrings

    str2bytes = lambda x: x if isinstance(x, bytes) else x.encode('utf-8')

    for l_row in ltable_array:
        lstring = l_row[l_join_attr_index]
        lstrings.push_back(str2bytes(lstring))
        # tokenize string and order the tokens using the token ordering
        lstring_tokens = order_using_token_ordering(
            tokenizer.tokenize(lstring), token_ordering)
        ltokens.push_back(lstring_tokens)

    cdef vector[vector[int]] rtokens
    cdef vector[string] rstrings

    for r_row in rtable_array:
        rstring = r_row[r_join_attr_index]
        rstrings.push_back(str2bytes(rstring))
        # tokenize string and order the tokens using the token ordering
        rstring_tokens = order_using_token_ordering(
            tokenizer.tokenize(rstring), token_ordering)
        rtokens.push_back(rstring_tokens)

    cdef CythonInvertedIndex prefix_index
    prefix_index.build_prefix_index(ltokens, tokenizer.qval, threshold)                                 
    
    cdef vector[vector[pair[int, int]]] output_pairs
    cdef vector[vector[double]] output_sim_scores                               
    cdef vector[pair[int, int]] partitions
    cdef int ii, rtable_size=len(rtable_array), partition_size, \
            partition_start=0, partition_end            

    partition_size = <int>(<float> rtable_size / <float> n_jobs)
    for ii in range(n_jobs):
        partition_end = partition_start + partition_size
        if partition_end > rtable_size or ii == n_jobs - 1:
            partition_end = rtable_size
        partitions.push_back(pair[int, int](partition_start, partition_end))
        partition_start = partition_end            
        output_pairs.push_back(vector[pair[int, int]]())
        output_sim_scores.push_back(vector[double]())                           

    cdef int qval = tokenizer.qval

    for ii in prange(n_jobs, nogil=True):    
        _ed_join_part(partitions[ii], ltokens, rtokens, qval, threshold, prefix_index, 
                     lstrings, rstrings, output_pairs[ii], output_sim_scores[ii])

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)
    
    for ii in range(n_jobs):    
        for jj in xrange(len(output_pairs[ii])):
            cur_outpair = output_pairs[ii][jj]
            l_row_inx = cur_outpair.first
            l_row = ltable_array[l_row_inx]
            r_row_inx = cur_outpair.second
            r_row = rtable_array[r_row_inx]

            if has_output_attributes:
                output_row = get_output_row_from_tables(
                                 l_row, r_row,
                                 l_key_attr_index, r_key_attr_index,
                                 l_out_attrs_indices,
                                 r_out_attrs_indices)
            else:
                output_row = [l_row[l_key_attr_index],
                              r_row[r_key_attr_index]]

            # if out_sim_score flag is set, append the edit distance 
            # score to the output record.
            if out_sim_score:
                output_row.append(output_sim_scores[ii][jj])

            output_rows.append(output_row)

#            if show_progress:
#                prog_bar.update()

    output_header = get_output_header_from_tables(
                        l_key_attr, r_key_attr,
                        l_out_attrs, r_out_attrs,
                        l_out_prefix, r_out_prefix)
    if out_sim_score:
        output_header.append("_sim_score")

    # generate a dataframe from the list of output rows
    output_table = pd.DataFrame(output_rows, columns=output_header)

    # If allow_missing flag is set, then compute all pairs with missing value in
    # at least one of the join attributes and then add it to the output         
    # obtained from the join. 
    if allow_missing:
        missing_pairs = get_pairs_with_missing_value(
                                            ltable, rtable,
                                            l_key_attr, r_key_attr,
                                            l_join_attr, r_join_attr,
                                            l_out_attrs, r_out_attrs,
                                            l_out_prefix, r_out_prefix,
                                            out_sim_score, show_progress)
        output_table = pd.concat([output_table, missing_pairs])

    # add an id column named '_id' to the output table.
    output_table.insert(0, '_id', range(0, len(output_table)))

    # revert the return_set flag of tokenizer, in case it was modified.
    if revert_tokenizer_return_set_flag:
        tokenizer.set_return_set(True)

    return output_table

cdef inline int int_min(int a, int b) nogil: return a if a <= b else b

cdef void _ed_join_part(pair[int, int] partition, 
                       vector[vector[int]]& ltokens, 
                       vector[vector[int]]& rtokens, 
                       int qval, double threshold, CythonInvertedIndex& index,
                       vector[string]& lstrings, vector[string]& rstrings, 
                       vector[pair[int, int]]& output_pairs,
                       vector[double]& output_sim_scores) nogil:    
    cdef oset[int] candidates                                      
    cdef vector[int] tokens
    cdef int j=0, m, i, prefix_length, cand                    
    cdef double edit_dist               
 
    for i in range(partition.first, partition.second):
        tokens = rtokens[i]                        
        m = tokens.size()                                                      
        prefix_length = int_min(<int>(qval * threshold + 1), m)                 
                                                                                
        for j in range(prefix_length):                                          
            if index.index.find(tokens[j]) == index.index.end():                
                continue
            for cand in index.index[tokens[j]]:                                             
                candidates.insert(cand)               

##        print i, candidate_overlap.size()                                      
        for cand in candidates:
            if m - threshold <= index.size_vector[cand] <= m + threshold:
                edit_dist = edit_distance(lstrings[cand], rstrings[i])                                         
                if edit_dist <= threshold:                                       
                    output_pairs.push_back(pair[int, int](cand, i))     
                    output_sim_scores.push_back(edit_dist)                          

        candidates.clear()

