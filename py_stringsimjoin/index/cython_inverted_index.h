#include <map>
#include <vector>

class CythonInvertedIndex {
  public:
    std::map<int, std::vector<int> > index;                       
    std::vector<int> size_vector;                                       

    CythonInvertedIndex();
    CythonInvertedIndex(std::map<int, std::vector<int> >&, std::vector<int>&);
    ~CythonInvertedIndex();
    void set_fields(std::map<int, std::vector<int> >&, std::vector<int>&);  
    void build_prefix_index(std::vector< std::vector<int> >&, int, double);
};
