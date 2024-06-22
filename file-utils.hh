#ifndef FILEUTILS_HH
#define FILEUTILS_HH

#include <string>
#include <vector>

int extractNumber(const std::string& filename, const std::string& prefix);
std::vector<std::string> find_files(const std::string& directory, const std::string& prefix);

#endif // FILEUTILS_HH