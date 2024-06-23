#ifndef FILEUTILS_HH
#define FILEUTILS_HH

#include <string>
#include <vector>

#include <seastar/core/future.hh>

int extractNumber(const std::string& filename, const std::string& prefix);
seastar::future<std::vector<std::string>>
find_files(const std::string& directory, const std::string& prefix);

#endif // FILEUTILS_HH