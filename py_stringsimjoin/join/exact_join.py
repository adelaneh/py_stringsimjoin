# exact join
from joblib import delayed, Parallel
import pandas as pd
import pyprind                                                                  

from py_stringsimjoin.index.hash_index import HashIndex
from py_stringsimjoin.utils.generic_helper import convert_dataframe_to_array, \
    find_output_attribute_indices, get_attrs_to_project, \
    get_num_processes_to_launch, get_output_header_from_tables, \
    get_output_row_from_tables, remove_redundant_attrs, split_table
from py_stringsimjoin.utils.missing_value_handler import \
    get_pairs_with_missing_value
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_attr_type, validate_key_attr, validate_input_table, \
    validate_output_attrs


def exact_join(ltable, rtable,
               l_key_attr, r_key_attr,
               l_join_attr, r_join_attr,
               allow_missing=False,
               l_out_attrs=None, r_out_attrs=None,
               l_out_prefix='l_', r_out_prefix='r_',
               n_jobs=1, show_progress=True):
    """Join two tables using attribute equivalence.

    Finds tuple pairs from left table and right table such that the value of
    the join attribute of a tuple from the left table exactly matches the 
    value of the join attribute of a tuple from the right table.
    This is similar to equi-join of two tables.

    Args:
        ltable (DataFrame): left input table.

        rtable (DataFrame): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table.

        r_join_attr (string): join attribute in right table.

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

    # check if the output attributes exist
    validate_output_attrs(l_out_attrs, ltable.columns,
                          r_out_attrs, rtable.columns)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

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
    
    if n_jobs <= 1:
        # if n_jobs is 1, do not use any parallel code.
        output_table = _exact_join_split(ltable_array, rtable_array,
                                         l_proj_attrs, r_proj_attrs, 
                                         l_key_attr, r_key_attr,
                                         l_join_attr, r_join_attr,
                                         l_out_attrs, r_out_attrs,
                                         l_out_prefix, r_out_prefix,
                                         show_progress)
    else:
        # if n_jobs is above 1, split the right table into n_jobs splits and
        # join each right table split with the whole of left table in a separate
        # process.
        r_splits = split_table(rtable_array, n_jobs)
        results = Parallel(n_jobs=n_jobs)(delayed(_exact_join_split)(
                                          ltable_array, r_splits[job_index],
                                          l_proj_attrs, r_proj_attrs,
                                          l_key_attr, r_key_attr,
                                          l_join_attr, r_join_attr,
                                          l_out_attrs, r_out_attrs,
                                          l_out_prefix, r_out_prefix,
                                      (show_progress and (job_index==n_jobs-1)))
                                          for job_index in range(n_jobs))
        output_table = pd.concat(results)

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
                                        False, show_progress) 
        output_table = pd.concat([output_table, missing_pairs])

    # add an id column named '_id' to the output table.
    output_table.insert(0, '_id', range(0, len(output_table)))

    return output_table


def _exact_join_split(ltable, rtable,                                                  
                      l_columns, r_columns, 
                      l_key_attr, r_key_attr,                                          
                      l_join_attr, r_join_attr,                                        
                      l_out_attrs=None, r_out_attrs=None,                              
                      l_out_prefix='l_', r_out_prefix='r_',                            
                      show_progress=True):   
    """Perform exact join for a split of ltable and rtable"""     
    # find column indices of key attr, join attr and output attrs in ltable     
    l_key_attr_index = l_columns.index(l_key_attr)                              
    l_join_attr_index = l_columns.index(l_join_attr)                            
    l_out_attrs_indices = find_output_attribute_indices(l_columns, l_out_attrs) 
                                                                                
    # find column indices of key attr, join attr and output attrs in rtable     
    r_key_attr_index = r_columns.index(r_key_attr)                              
    r_join_attr_index = r_columns.index(r_join_attr)                            
    r_out_attrs_indices = find_output_attribute_indices(r_columns, r_out_attrs) 
                                                                                
    # Build hash index over ltable                                          
    hash_index = HashIndex(ltable, l_join_attr_index)             
    hash_index.build()

    output_rows = []                                                            
    has_output_attributes = (l_out_attrs is not None or                         
                             r_out_attrs is not None)                           
                                                                                
    if show_progress:                                                           
        prog_bar = pyprind.ProgBar(len(rtable))                            
                                                                                
    for r_row in rtable:
        # probe hash index to obtain matching records from ltable                                                   
        matches = hash_index.probe(r_row[r_join_attr_index])

        # add output rows
        for l_idx in matches:
            if has_output_attributes:                                       
                output_row = get_output_row_from_tables(                    
                                     ltable[l_idx], r_row,                  
                                     l_key_attr_index, r_key_attr_index,        
                                     l_out_attrs_indices, r_out_attrs_indices)  
            else:                                                           
                output_row = [ltable[l_idx][l_key_attr_index], 
                              r_row[r_key_attr_index]]                      
                                                                                
            output_rows.append(output_row)   

        if show_progress:                                                       
            prog_bar.update()                                                   
                                                                                
    output_header = get_output_header_from_tables(l_key_attr, r_key_attr,       
                                                  l_out_attrs, r_out_attrs,     
                                                  l_out_prefix, r_out_prefix)   
                                                                                
    output_table = pd.DataFrame(output_rows, columns=output_header)             
    return output_table         
