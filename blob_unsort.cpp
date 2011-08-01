
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include <iostream>
#include <fstream>
#include <string>

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include <crypto++/sha.h>

#include <eblob/blob.h>

#include "common.h"


boost::iostreams::mapped_file file_;
std::map<uint64_t, struct eblob_disk_control *> blob_index;

void open_index(std::string &path) {
	std::ostringstream filename;
	struct eblob_disk_control *dc;
	uint64_t index_pos;


	file_.open(path, std::ios_base::in | std::ios_base::binary);

	index_pos = 0;
	while (index_pos < file_.size()) {

		dc = (struct eblob_disk_control *)(file_.const_data() + index_pos);

		blob_index[dc->position] = dc;

		index_pos += sizeof(struct eblob_disk_control);
	}
}

int main(int argc, char *argv[])
{
	std::map<uint64_t, struct eblob_disk_control *>::iterator iter;
	struct eblob_disk_control *dc;

	try {
		if (argc != 3) {
			std::ostringstream error_text;
			error_text << "Usage: " << argv[0] << " input_blob output_blob";

			throw std::runtime_error(error_text.str());
		}

		std::string input_path(argv[1]);

		std::ofstream unsorted_index(argv[2], std::ios::out | std::ios::binary);

		open_index(input_path);

		std::cout << "loaded " << blob_index.size() << " elements" << std::endl;

		for (iter = blob_index.begin(); iter != blob_index.end(); iter++) {
			dc = iter->second;
			unsorted_index.write((char *)dc, sizeof(struct eblob_disk_control));
		}

		unsorted_index.close();
		file_.close();

	} catch (const std::exception &e) {
		std::cerr << "Exiting: " << e.what() << std::endl;
		return -1;
	}
}
