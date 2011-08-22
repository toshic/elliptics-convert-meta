
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
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
#include <boost/date_time/posix_time/posix_time.hpp>
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
		boost::shared_ptr<boost::iostreams::mapped_file> file;
};

class generic_processor {
	public:
		virtual processor_key next(void) = 0;
};


struct timespec parse_time(std::string &datetime)
{
	struct timespec datetime_dt;
	try {
		boost::posix_time::ptime ptime(boost::posix_time::time_from_string(datetime));
		boost::posix_time::ptime epoch(boost::gregorian::date(1970,1,1));
		boost::posix_time::time_duration diff;

		diff = ptime - epoch;

		datetime_dt.tv_sec = diff.total_seconds();
		datetime_dt.tv_nsec = diff.total_nanoseconds();
	} catch(...) {
		datetime_dt.tv_sec = 0;
		datetime_dt.tv_nsec = 0;
	}

	return datetime_dt;
}

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
					key.file = data_file_;
					break;
				}
			}
			return key;
		}

	private:
		std::string path_;
		int index_;
		uint64_t pos_;
		boost::iostreams::mapped_file file_;
		boost::shared_ptr<boost::iostreams::mapped_file> data_file_;

		void open_index() {
			std::ostringstream filename;
			struct eblob_disk_control *dc;
			uint64_t index_pos;

			pos_ = 0;

			if (file_.is_open())
				file_.close();

			filename << path_ << "." << index_;
			data_file_.reset(new boost::iostreams::mapped_file(filename.str(), std::ios_base::in | std::ios_base::binary));

			filename << ".index";
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
				key.size = fs::file_size(itr_->path());

				if (key.size == 0) {
					++itr_;
					continue;
				}

				std::cout << "fs: " << itr_->path() << std::endl;
				key.file.reset(new boost::iostreams::mapped_file(key.path, std::ios_base::in | std::ios_base::binary));

				++itr_;
				break;
			}
			return key;
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
		remote_update(const std::vector<int> groups, const std::string meta, struct timespec update_date) :
				 groups_(groups), meta_(meta), update_date_(update_date), aflags_(0) {
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

			total_cnt = 0;

			memset(&ecfg, 0, sizeof(ecfg));
			ecfg.file = (char *)meta_.c_str();
			ecfg.sync = 30;

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
				eblob_cleanup(meta);
				delete proc;
				std::cerr << "Totally processed " << total_cnt << " records" << std::endl;
				throw e;
			}
			std::cerr << "1Totally processed " << total_cnt << " records" << std::endl;
			eblob_cleanup(meta);

			delete proc;
		}

	private:
		std::vector<int> groups_;
		std::string meta_;
		boost::mutex data_lock_;
		int aflags_;
		uint64_t total_cnt;
		struct timespec update_date_;

		void update(generic_processor *proc, processor_key &key, struct eblob_backend *meta) {
			struct dnet_raw_id id;
			struct dnet_meta *m;
			struct dnet_meta_container mc;
			struct dnet_meta_checksum *csum;
			struct dnet_meta *mp;
			uint8_t checksum[DNET_CSUM_SIZE];
			int err;

			if (key.offset + key.size > key.file->size()) {
				std::cout << "failed. " << dnet_dump_id_str(id.id) << ": incorrect length: "
				<< "offset=" << key.offset << ", size=" << key.size << ", file.size=" << key.file->size() << std::endl;
				return;
			}

			memset(&mc, 0, sizeof(mc));

			memcpy(&mc.id, (unsigned char *)key.id.data(), DNET_ID_SIZE);
			std::cout << "Processing " << dnet_dump_id_len(&mc.id, DNET_ID_SIZE) << " ";

			memcpy(&id.id, (unsigned char *)key.id.data(), DNET_ID_SIZE);
			err = dnet_db_read_raw(meta, &id, &mc.data);
			if (err == -ENOENT) {
				struct dnet_meta_create_control ctl;

				std::cout << "not found. Re-creating metadata" << std::endl;

				memset(&ctl, 0, sizeof(ctl));

				ctl.obj = NULL;
				ctl.len = 0;

				ctl.groups = &groups_[0];
				ctl.group_num = groups_.size();

				if (!(aflags_ & DNET_ATTR_NOCSUM)) {
					eblob_hash(meta, ctl.checksum, sizeof(ctl.checksum), key.file->const_data() + key.offset, key.size);
				}

				dnet_setup_id(&ctl.id, 0, id.id);

				ctl.ts = update_date_;

				err = dnet_create_write_meta(&ctl, &mc.data);
				if (err <= 0) {
					std::cout << "Metadata re-creating failed! err: " << err << std::endl;
					return;
				}

				mc.size = err;
				err = dnet_db_write_raw(meta, &id, mc.data, mc.size);
				if (err) {
					std::cout << "Metadata write failed! err: " << err << std::endl;
				}

			} else if (err <= 0) {
				std::cout << "failed. " << dnet_dump_id_str(id.id) << ": meta DB read failed, err: " << err << std::endl;
				return;
			} else if (err > 0 && !(aflags_ & DNET_ATTR_NOCSUM)) {
				mc.size = err;

				mp = dnet_meta_search_cust(&mc, DNET_META_CHECKSUM);
				if (mp) {
					csum = (struct dnet_meta_checksum *)mp->data;
					eblob_hash(meta, checksum, sizeof(checksum), key.file->const_data() + key.offset, key.size);
					if (memcmp(csum->checksum, checksum, DNET_CSUM_SIZE)) {
						std::cout << "Checksum mismatch, updating with the new one" << std::endl;

						memcpy(csum->checksum, checksum, DNET_CSUM_SIZE);
						dnet_current_time(&csum->tm);
						dnet_convert_meta_checksum(csum);

						err = dnet_db_write_raw(meta, &id, mc.data, mc.size);
						if (err) {
							std::cout << "Metadata write failed! err: " << err << std::endl;
						}
					}
				}

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
						total_cnt++;
					}

					update(proc, key, meta);
				}
			} catch (const std::exception &e) {
				std::cerr << "Catched exception : " << e.what() << std::endl;
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
		std::string update_date;
		struct timespec update_dt;
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
			("enable-checksum", po::value<int>(&csum_enabled)->default_value(0),
			 	"Set to 1 if you want to enable server generated checksums")
			("update-date", po::value<std::string>(&update_date)->default_value(""),
				"Update date for created meta in format like \"2011-08-22 21:42:00\"")
		;

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if (vm.count("help") || !vm.count("input-path") || !vm.count("meta")) {
			std::cout << desc << "\n";
			return -1;
		}

		update_dt = parse_time(update_date);
		remote_update up(groups, meta, update_dt);
		up.process(vm["input-path"].as<std::string>(), thread_num, csum_enabled);
	} catch (const std::exception &e) {
		std::cerr << "Exiting: " << e.what() << std::endl;
	}
}
