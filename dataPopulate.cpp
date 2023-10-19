// Copyright Hiep Cao 2023
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <random>

unsigned int seed1, seed2, seed3, seed4;

// Employee structure
struct Employee {
    std::string name;
    std::string dateOfBirth;
    char gender;
    std::string daysAvailable;
};

// Function to generate random date of birth
std::string randomDateOfBirth() {
    int year = rand_r(&seed1) % 51 + 1950;  // Random year between 1950 and 2000
    int month = rand_r(&seed2) % 12 + 1;
    int day = rand_r(&seed3) % 28 + 1;  // Assume 28 days in a month 
    std::stringstream ss;
    ss << year << "-" << (month < 10 ? "0" : "") 
    << month << "-" << (day < 10 ? "0" : "") << day;
    return ss.str();
}

// Function to generate random gender ('F' or 'M')
char randomGender() {
    return rand() % 2 == 0 ? 'F' : 'M';
}

// Function to generate random days available in a week
std::string randomDaysAvailable() {
    std::vector<std::string> days = 
    { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday" };
    auto rng = std::default_random_engine {};
    std::shuffle(days.begin(), days.end(), rng);
    int numDays = rand_r(&seed4) % 5 + 1;  // Random
    std::string result;
    for (int i = 0; i < numDays; ++i) {
        result += days[i];
        if (i < numDays - 1) {
            result += ",";
        }
    }
    return result;
}

// Function to clear previous data from the CSV file
void clearPreviousData(const std::string& filename) {
    std::ofstream ofs(filename, std::ios::trunc);
    ofs.close();
}

int populateData() {
    srand(static_cast<unsigned>(time(nullptr)));

    const std::string filename = "employee.csv";
    const int numEntries = 1000;

    clearPreviousData(filename);

    std::ofstream outputFile(filename, std::ios::app);
    outputFile << "Name,Date of Birth,Gender,Days Available\n";

    for (int i = 0; i < numEntries; ++i) {
        Employee employee;
        employee.name = "Employee " + std::to_string(i + 1);
        employee.dateOfBirth = randomDateOfBirth();
        employee.gender = randomGender();
        employee.daysAvailable = randomDaysAvailable();

        outputFile << employee.name << "," << employee.dateOfBirth << "," 
        << employee.gender << "," << employee.daysAvailable << "\n";
    }

    outputFile.close();

    std::cout << "CSV file '" << filename << "' with " << numEntries 
    << " entries has been generated.\n";

    return 0;
}
