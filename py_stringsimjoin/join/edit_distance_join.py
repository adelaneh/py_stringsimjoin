# edit distance join
import py_stringsimjoin
import py_stringsimjoin.join.edit_distance_join_py as edjpy
import py_stringsimjoin.join.edit_distance_join_cy as edjcy
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer

def edit_distance_join(ltable, rtable,
                       l_key_attr, r_key_attr,
                       l_join_attr, r_join_attr,
                       threshold, comp_op='<=',
                       allow_missing=False,
                       l_out_attrs=None, r_out_attrs=None,
                       l_out_prefix='l_', r_out_prefix='r_',
                       out_sim_score=True, n_jobs=1, show_progress=True,
                       tokenizer=QgramTokenizer(qval=2)):
    if py_stringsimjoin.__use_cython__:
        return edjcy.edit_distance_join(ltable, rtable,
                       l_key_attr, r_key_attr,
                       l_join_attr, r_join_attr,
                       threshold, comp_op,
                       allow_missing,
                       l_out_attrs, r_out_attrs,
                       l_out_prefix, r_out_prefix,
                       out_sim_score, n_jobs, show_progress,
                       tokenizer)
    else:
        return edjpy.edit_distance_join(ltable, rtable,
                       l_key_attr, r_key_attr,
                       l_join_attr, r_join_attr,
                       threshold, comp_op,
                       allow_missing,
                       l_out_attrs, r_out_attrs,
                       l_out_prefix, r_out_prefix,
                       out_sim_score, n_jobs, show_progress,
                       tokenizer)
