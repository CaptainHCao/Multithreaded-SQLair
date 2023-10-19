/* copyright caohd 2023
 * A very lightweight (light as air) implementation of a simple CSV-based 
 * database system that uses SQL-like syntax for querying and updating the
 * CSV files.
 * 
 */

#include <string>
#include <fstream>
#include <tuple>
#include <algorithm>
#include <memory>
#include "SQLAir.h"
#include "HTTPFile.h"
#include <boost/format.hpp>

using namespace boost::asio::ip;

// A shortcut to refer to scoped_lock in code
using Guard = std::scoped_lock<std::mutex>;

/**
 * A fixed HTTP response header that is used by the runServer method below.
 * Note that this a constant (and not a global variable)
 */
// const std::string HTTPRespHeader = "HTTP/1.1 200 OK\r\n"
//     "Server: localhost\r\n"
//     "Connection: Close\r\n"
//     "Content-Type: text/plain\r\n"
//     "Content-Length: ";

const std::string HTTPRespHeader = "HTTP/1.1 200 OK\r\n"
    "Server: SimpleServer\r\n"
    "Content-Length: %1%\r\n"
    "Connection: Close\r\n"
    "Content-Type: text/html\r\n\r\n";

// Helper method to process each row in select queries
void SQLAir::selectRowProcess(CSV& csv, StrVec colNames, 
        const int whereColIdx, const std::string& cond, 
        const std::string& value, 
        std::string& rowText, int& rowCount) {
    for (auto& row : csv) {
        // lock the row
        Guard g(row.rowMutex);
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        bool isMatch  = (whereColIdx == -1) ? true : 
        matches(row.at(whereColIdx), cond, value);
        
        if (isMatch) {
            std::string delim = "";
            for (auto& colName : colNames) {
                std::string cell;
                cell = row.at(csv.getColumnIndex(colName));
                rowText += delim + cell;
                delim = "\t";
            }
            // os << std::endl;
            rowText += "\n";
            rowCount++;
        }
    }
}

// Helper method to process each row in update queries
void SQLAir::updateRowProcess(CSV& csv, StrVec colNames,  StrVec values, 
        const int whereColIdx, const std::string& cond, 
        const std::string& value, int& rowCount) {
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        Guard g(row.rowMutex);
        bool isMatch;
        if (whereColIdx != -1) {
            isMatch = matches(row.at(whereColIdx), cond, value);
        }
        if (whereColIdx == -1 || isMatch) {
            for (size_t i = 0; i < colNames.size(); i++) {
                // update each cell
                row.at(csv.getColumnIndex(colNames[i])) = values.at(i);
            }
            rowCount++;
        }
    }        
}

// API method to perform operations associated with a "select" statement
// to print columns that match an optional condition.
void SQLAir::selectQuery(CSV& csv, bool mustWait, StrVec colNames, 
        const int whereColIdx, const std::string& cond, 
        const std::string& value, std::ostream& os) {
    // Convert any "*" to suitable column names. See CSV::getColumnNames() 
    if (colNames[0] == "*") colNames = csv.getColumnNames();

    // First print the column names.
    // os << colNames << std::endl;

    // row count
    int rowCount = 0;
    std::string rowText;
    // Print each row that matches an optional condition.
    selectRowProcess(csv, colNames, whereColIdx, cond, value, rowText, 
            rowCount);
    while (rowCount == 0 && mustWait) {
        // unique lock
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        csv.csvCondVar.wait(lock);
        selectRowProcess(csv, colNames, whereColIdx, cond, value, rowText, 
            rowCount);  // critical section
        if (rowCount != 0) {
            mustWait = false;
        }
    }
    if (rowCount != 0) os << colNames << std::endl;
    os << rowText <<std::to_string(rowCount) + " row(s) selected." << std::endl;
}

void
SQLAir::updateQuery(CSV& csv, bool mustWait, StrVec colNames, StrVec values, 
        const int whereColIdx, const std::string& cond, 
        const std::string& value, std::ostream& os)  {
    // row count
    int rowCount = 0;

    // Print each row that matches an optional condition.
    updateRowProcess(csv, colNames, values, 
            whereColIdx, cond, value, rowCount);
    while (rowCount == 0 && mustWait) {
        // unique lock
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        csv.csvCondVar.wait(lock);
        updateRowProcess(csv, colNames, values, 
            whereColIdx, cond, value , rowCount);  // critical section

        if (rowCount != 0) { 
            mustWait = false;
        }
    }
    if (rowCount != 0) { 
        csv.csvCondVar.notify_all();
    }
    os << std::to_string(rowCount) + " row(s) updated." << std::endl;
}

void 
SQLAir::insertQuery(CSV& csv, bool mustWait, StrVec colNames, 
        StrVec values, std::ostream& os) {
    throw Exp("insert is not yet implemented.");
}

void 
SQLAir::deleteQuery(CSV& csv, bool mustWait, const int whereColIdx, 
        const std::string& cond, const std::string& value, std::ostream& os) {
    throw Exp("delete is not yet implemented.");
}

// Thread method for each thread
void SQLAir::clientThread(std::istream& is, std::ostream& os) {  
    std::string line, path;
    is >> line >> path;
    // Skip/ignore all the HTTP request & headers for now.
    if (path.find("/sql-air?query=") != std::string::npos) {
        // This is a command to be processed. So use a helper method
        // to streamline the code.
        for (std::string hdr; std::getline(is, hdr) && !hdr.empty()
            && hdr != "\r"; ) {}
        std::ostringstream oss;
        path = path.substr(15);  // get rid of the syntax at the begin ?
        path = Helper::url_decode(path);
        try {
            SQLAirBase::process(path, oss);
        }  catch (const std::exception &exp) {
            oss  << "Error: " << exp.what() << std::endl;
        }
        std::string html = oss.str();
        auto httpRespHdr = boost::str(boost::format(HTTPRespHeader) 
                    % html.length());
        os << httpRespHdr << html;   
    } else if (!path.empty()) {
        // In this case we assume the user is asking for a file. Have
        // the helper http class do the processing.
        // Remove the leading '/' sign.
        path = path.substr(1); 
        // Use the http::file helper method to send the response back
        // to the client.
        path = Helper::url_decode(path);
        os << http::file(path);
    }
    // decrease thread count
    SQLAir::numThreads--;
    SQLAir::thrCond.notify_one();  // end client thread by notify empty slot
}

// The method to have this class run as a web-server. 
void 
SQLAir::runServer(boost::asio::ip::tcp::acceptor& server, const int maxThr) {
    // unique lock for the server 
    std::unique_lock<std::mutex> lock(SQLAir::serverMutex);
    while (true) {
        // we have 2 threads when starting the .exe
        SQLAir::thrCond.wait(lock, [&] 
                                 {return SQLAir::numThreads <= maxThr; });
        // Setup a server socket to accept connections on the socket
        // Create garbage-collected, shared object on heap so we can
        // send it to another thread and not worry about life-time of
        // the socket connection.
        TcpStreamPtr client = std::make_shared<tcp::iostream>();
        // Wait for a client to connect
        server.accept(*client->rdbuf());
        // Process request from client on a separate background thread
        std::thread thr([this, client]()
            { SQLAir::clientThread(*client, *client);});
        thr.detach();  
        // increase thread count
        SQLAir::numThreads++;
    }
}

// Assume that an URL of the form http://data.com:8080/test.csv has been
// split into host = “data.com”, port = “8080” and path = “/test.csv”
void SQLAir::loadFromURL(CSV& csv, const std::string& hostName, 
        const std::string& port, const std::string& path) {
    // Setup a boost tcp stream to send an HTTP request to the web-server
    
    tcp::iostream client(hostName, port);

    if (!client.good()) {
        throw Exp("Unable to connect to " + hostName + " at port " + port);
    }
    // Send an HTTP get request to get the data from the server
    client << "GET " << path << " HTTP/1.1\r\nHost: " << hostName << "\r\n"
        << "Connection: Close\r\n\r\n";
    // Get response status from server to ensure we have a valid response.
    std::string status; 
    // To check if response is 200 OK status code.
    std::getline(client, status);
    // throw exception if do not find 200 OK
    if (status.find("200") ==  std::string::npos) {
       throw Exp("Error (" + Helper::trim(status) + ") getting " + path + 
       " from " + hostName + " at port " + port);
    } 
    // Skip headers
    for (std::string hdr; std::getline(client, hdr) && !hdr.empty()
        && hdr != "\r"; ) {}
    // Now have the CSV class do rest of the processing
    csv.load(client);
}

CSV& SQLAir::loadAndGet(std::string fileOrURL) {
    // Check if the specified fileOrURL is already loaded in a thread-safe
    // manner to avoid race conditions on the unordered_map
    {
        std::scoped_lock<std::mutex> guard(recentCSVMutex);
        // Use recent CSV if parameter was empty string.
        fileOrURL = (fileOrURL.empty() ? recentCSV : fileOrURL);
        // Update the most recently used CSV for the next round
        recentCSV = fileOrURL;
        if (inMemoryCSV.find(fileOrURL) != inMemoryCSV.end()) {
            // Requested CSV is already in memory. Just return it.
            return inMemoryCSV.at(fileOrURL);
        }
    }
    // When control drops here, we need to load the CSV into memory.
    // Loading or I/O is being done outside critical sections
    CSV csv;   // Load data into this csv
    if (fileOrURL.find("http://") == 0) {
        // This is an URL. We have to get the stream from a web-server
        // Implement this feature.
        std::string host, port, path;
        std::tie(host, port, path) = Helper::breakDownURL(fileOrURL);
        loadFromURL(csv, host, port, path);
    } else {
        // We assume it is a local file on the server. Load that file.
        std::ifstream data(fileOrURL);
        // This method may throw exceptions on errors.
        csv.load(data);
    }
    
    // We get to this line of code only if the above if-else to load the
    // CSV did not throw any exceptions. In this case we have a valid CSV
    // to add to our inMemoryCSV list. We need to do that in a thread-safe
    // manner.
    std::scoped_lock<std::mutex> guard(recentCSVMutex);
    // Move (instead of copy) the CSV data into our in-memory CSVs
    inMemoryCSV[fileOrURL].move(csv);
    // Return a reference to the in-memory CSV (not temporary one)
    return inMemoryCSV.at(fileOrURL);
}

// Save the currently loaded CSV file to a local file.
void 
SQLAir::saveQuery(std::ostream& os) {
    if (recentCSV.empty() || recentCSV.find("http://") == 0) {
        throw Exp("Saving CSV to an URL using POST is not implemented");
    }
    // Create a local file and have the CSV write itself.
    std::ofstream csvData(recentCSV);
    inMemoryCSV.at(recentCSV).save(csvData);
    os << recentCSV << " saved.\n";
}
