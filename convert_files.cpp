
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include <iostream>
#include <string>

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include <eblob/blob.h>

#include "common.h"

//using namespace zbr;

class processor_key {
	public:
		std::string path;
		uint64_t offset;
		uint64_t size;
		std::string id;
};

class generic_processor {
	public:
		virtual processor_key next(void) = 0;
		virtual void move(processor_key &) = 0;
		virtual void remove(processor_key &) = 0;
};

class eblob_processor : public generic_processor {
	public:
		eblob_processor(const std::string &path) : path_(path), index_(0) {
			open_index();
		}

		virtual ~eblob_processor() {
			if (file_.is_open())
				file_.close();
		}

		processor_key next(void) {
			struct eblob_disk_control dc;
			processor_key key;

			while (true) {
				if (pos_ >= file_.size()) {
					open_index();
				}

				memcpy(&dc, file_.const_data() + pos_, sizeof(dc));

				std::cout << "offset: " << dc.position << ", size: " << dc.data_size <<
					", disk_size: " << dc.disk_size << std::endl;

				pos_ += sizeof(dc);

				if (!(dc.flags & BLOB_DISK_CTL_REMOVE)) {
					key.id.assign((char *)dc.key.id, sizeof(dc.key.id));
					key.path = path_ + "." + boost::lexical_cast<std::string>(index_ - 1);
					key.offset = dc.position + sizeof(dc);
					key.size = dc.data_size;
					break;
				}
			}

			return key;
		}

		void move(processor_key &) {
		}
		void remove(processor_key &) {
		}

	private:
		std::string path_;
		int index_;
		uint64_t pos_;
		boost::iostreams::mapped_file file_;

		void open_index() {
			std::ostringstream filename;

			pos_ = 0;

			if (file_.is_open())
				file_.close();

			filename << path_ << "." << index_ << ".index";
			file_.open(filename.str(), std::ios_base::in | std::ios_base::binary);

			++index_;
		}
};

class fs_processor : public generic_processor {
	public:
		fs_processor(const std::string &path) : itr_(fs::path(path)) {
		}

		processor_key next(void) {
			processor_key key;

			while (true) {
				if (itr_ == end_itr_) {
					throw std::runtime_error("Whole directory has been traversed");
				}

				if (fs::is_directory(itr_->path())) {
					++itr_;
					continue;
				}

				if (itr_->leaf().size() != DNET_ID_SIZE * 2) {
					++itr_;
					continue;
				}

				parse(itr_->leaf(), key.id);
				key.path = itr_->path().string();
				key.offset = 0;
				key.size = 0;

				std::cout << "fs: " << itr_->path() << std::endl;

				++itr_;
				break;
			}
			return key;
		}

		void move(processor_key &k) {
			char dstr[DNET_ID_SIZE*2+1];
			dnet_dump_id_len_raw((const unsigned char *)k.id.data(), DNET_ID_SIZE, dstr);

			std::string dst = "/tmp/";
			dst.append(dstr);

			fs::rename(k.path, dst);

			std::cout << "moved " << k.path << " -> " << dst << std::endl;
			k.path = dst;
		}

		void remove(processor_key &k) {
			fs::remove(k.path);
		}

	private:
		fs::recursive_directory_iterator end_itr_, itr_;
		std::vector<std::string> dirs_;

		void parse(const std::string &value, std::string &key) {
			unsigned char ch[5];
			unsigned int i, len = value.size();
			unsigned char id[DNET_ID_SIZE];

			memset(id, 0, DNET_ID_SIZE);

			if (len/2 > DNET_ID_SIZE)
				len = DNET_ID_SIZE * 2;

			ch[0] = '0';
			ch[1] = 'x';
			ch[4] = '\0';
			for (i=0; i<len / 2; i++) {
				ch[2] = value[2*i + 0];
				ch[3] = value[2*i + 1];

				id[i] = (unsigned char)strtol((const char *)ch, NULL, 16);
			}

			if (len & 1) {
				ch[2] = value[2*i + 0];
				ch[3] = '0';

				id[i] = (unsigned char)strtol((const char *)ch, NULL, 16);
			}

			key.assign((char *)id, DNET_ID_SIZE);
		}

};

class remote_update {
	public:
		remote_update(const std::vector<int> groups, const std::string meta) : groups_(groups), meta_(meta), aflags_(0) {
		}

		void process(const std::string &path, int tnum = 16, int csum_enabled = 0) {
			generic_processor *proc;
			struct eblob_backend *meta = NULL;
			struct eblob_config ecfg;
			struct eblob_log log;

			if (!csum_enabled)
				aflags_ |= DNET_ATTR_NOCSUM;

			if (fs::is_directory(fs::path(path))) {
				proc = new fs_processor(path);
			} else {
				proc = new eblob_processor(path);
			}

			memset(&ecfg, 0, sizeof(ecfg));
			ecfg.file = (char *)meta_.c_str();

			log.log = dnet_common_log;
			log.log_private = NULL;
			log.log_mask = EBLOB_LOG_ERROR | EBLOB_LOG_INFO | EBLOB_LOG_NOTICE;
			ecfg.log = &log;

			meta = eblob_init(&ecfg);
			if (!meta) {
				std::cerr << "Failed to open meta database" << meta_ << std::endl;
				throw std::runtime_error("Failed to open meta database");
			}

			try {
				boost::thread_group threads;
				for (int i=0; i<tnum; ++i) {
					threads.create_thread(boost::bind(&remote_update::process_data, this, proc, meta));
				}

				threads.join_all();
			} catch (const std::exception &e) {
				std::cerr << "Finished processing " << path << " : " << e.what() << std::endl;
				delete proc;
				throw e;
			}

			delete proc;
		}

	private:
		std::vector<int> groups_;
		std::string meta_;
		boost::mutex data_lock_;
		int aflags_;

		void update(generic_processor *proc, processor_key &key, struct eblob_backend *meta) {
			struct dnet_raw_id id;
			struct dnet_meta *m;
			struct dnet_meta_container mc;
			int err;

			memset(&mc, 0, sizeof(mc));

			//std::string meta, data;

			memcpy(&mc.id, (unsigned char *)key.id.data(), DNET_ID_SIZE);
			std::cout << "Processing " << dnet_dump_id_len(&mc.id, DNET_ID_SIZE) << " ";

			memcpy(&id.id, (unsigned char *)key.id.data(), DNET_ID_SIZE);
			err = dnet_db_read_raw(meta, &id, &mc.data);
			if (err == -ENOENT) {
				struct dnet_metadata_control ctl;

				std::cout << "not found. Re-creating metadata" << std::endl;

				memset(&ctl, 0, sizeof(ctl));

				ctl.obj = NULL;
				ctl.len = 0;

				ctl.groups = &groups_[0];
				ctl.group_num = groups_.size();

				dnet_setup_id(&ctl.id, 0, id.id);

				err = dnet_create_write_meta(&ctl, &mc.data);
				if (err <= 0) {
					std::cout << "Metadata re-creating failed! err: " << err << std::endl;
					return;
				}

				err = dnet_db_write_raw(meta, &id, mc.data, mc.size);
				if (err) {
					std::cout << "Metadata write failed! err: " << err << std::endl;
				}

			} else if (err <= 0) {
				std::cout << "failed. " << dnet_dump_id_str(id.id) << ": meta DB read failed, err: " << err << std::endl;
				return;
			}

			free(mc.data);
		}

		void process_data(generic_processor *proc, struct eblob_backend *meta) {
			try {
				while (true) {
					processor_key key;

					{
						boost::mutex::scoped_lock scoped_lock(data_lock_);
						key = proc->next();
					}

					update(proc, key, meta);
				}
			} catch (...) {
			}
		}
};

int main(int argc, char *argv[])
{
	try {
		namespace po = boost::program_options;
		po::options_description desc("Options (required options are marked with *");
		int groups_array[] = {1};
		std::vector<int> groups(groups_array, groups_array + ARRAY_SIZE(groups_array));
		std::string addr;
		std::string meta;
		int port, family;
		int thread_num;
		int csum_enabled;

		desc.add_options()
			("help", "This help message")
			("input-path", po::value<std::string>(), "Input path (*)")
			("threads", po::value<int>(&thread_num)->default_value(16), "Number of threads to iterate over input data")
			("group", po::value<std::vector<int> >(&groups),
			 	"Group number which will host given object, can be used multiple times for several groups")
			("meta", po::value<std::string>(&meta), "Meta DB")
			//("enable-checksum", po::value<int>(&csum_enabled)->default_value(0),
			// 	"Set to 1 if you want to enable server generated checksums")
		;

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if (vm.count("help") || !vm.count("input-path") || !vm.count("meta")) {
			std::cout << desc << "\n";
			return -1;
		}

		remote_update up(groups, meta);
		up.process(vm["input-path"].as<std::string>(), thread_num, csum_enabled);
	} catch (const std::exception &e) {
		std::cerr << "Exiting: " << e.what() << std::endl;
	}
}
