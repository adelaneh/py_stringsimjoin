
from libcpp.vector cimport vector                                               
from libcpp.map cimport map as omap                                             

cdef extern from "cython_inverted_index.h" nogil:                                      
    cdef cppclass CythonInvertedIndex nogil:                                          
        CythonInvertedIndex()                                                         
        CythonInvertedIndex(omap[int, vector[int]]&, vector[int]&)                    
        void set_fields(omap[int, vector[int]]&, vector[int]&)                  
        void build_prefix_index(vector[vector[int]]&, int, double)                  
        omap[int, vector[int]] index                                            
        vector[int] size_vector                
