#include "cython_inverted_index.h"

CythonInvertedIndex::CythonInvertedIndex(std::map<int, std::vector<int> >& ind, std::vector<int>& sv) {
    index = ind;
    size_vector = sv;
}

void CythonInvertedIndex::set_fields(std::map<int, std::vector<int> >& ind, std::vector<int>& sv) {
    index = ind;                                                                  
    size_vector = sv;                                                             
}

void CythonInvertedIndex::build_prefix_index(std::vector< std::vector<int> >& token_vectors, int qval, double threshold) {
    index = std::map<int, std::vector<int> >();
    size_vector = std::vector<int>();                                        

    int m, n=token_vectors.size(), prefix_length;                     

    for (int ii  = 0; ii < n; ii++) {
        std::vector<int> tokens = token_vectors[ii];                                               
        m = tokens.size();                                                      
        size_vector.push_back(m);                                               
        prefix_length = std::min((int)(qval * threshold + 1), m);                
                                                                                
        for (int jj = 0; jj < prefix_length; jj++)                                          
            index[tokens[jj]].push_back(ii);                                       
    }
}

CythonInvertedIndex::CythonInvertedIndex() {}

CythonInvertedIndex::~CythonInvertedIndex() {} 
