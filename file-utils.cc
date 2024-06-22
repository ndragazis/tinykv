#include <iostream>
#include <filesystem>
#include <regex>

#include "file-utils.hh"

namespace fs = std::filesystem;

// Extract numeric identifier from filename
int extractNumber(const std::string& filename, const std::string& prefix) {
    std::regex re(prefix + "(\\d+)");
    std::smatch match;
    if (std::regex_search(filename, match, re) && match.size() > 1) {
        return std::stoi(match.str(1));
    }
    return -1;
}

// Find files with a specific prefix
std::vector<std::string> find_files(const std::string& directory, const std::string& prefix) {
    std::vector<std::string> matchingFiles;
    try {
        for (const auto& entry : fs::directory_iterator(directory)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.find(prefix) == 0) { // Check if the prefix matches
                    matchingFiles.push_back(entry.path().string());
                }
            }
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    return matchingFiles;
}