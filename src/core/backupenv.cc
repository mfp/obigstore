
#include <leveldb/env.h>
#include <leveldb/status.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>

using namespace leveldb;

// from leveldb util/env_posix.cc

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

class PosixWritableFile : public WritableFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~PosixWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  virtual Status Append(const Slice& data) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||
        fdatasync(fileno(file_)) != 0) {
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};


// from leveldb  port/port.h

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  void Unlock();
  void AssertHeld() { }

 private:
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

// from leveldb  port/port_posix.cc

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); }

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

// refer to http://code.google.com/p/leveldb/issues/detail?id=184 and
// https://github.com/titanous/HyperLevelDB/commit/344276355f4753317522371fe55823e6b29805e7

class BackupEnv : public EnvWrapper {
 private:
  Mutex mu_;
  bool backing_up_;
  std::vector<std::string> deferred_deletions_;

 public:
  explicit BackupEnv(Env* t)
      : EnvWrapper(t), backing_up_(false) {
  }

  virtual Status NewWritableFile(const std::string& fname, WritableFile** result);

  Status DeleteFile(const std::string& f);

  // Call (*save)(arg, filename, length) for every file that
  // should be backed up to construct a consistent view of the
  // database.  "length" may be negative to indicate that the entire
  // file must be copied.  Otherwise the first "length" bytes must be
  // copied.
  Status Backup(const std::string& dir,
                int (*func)(void*, const char* fname, uint64_t length),
                void* arg);

  bool CopyFile(const std::string& fname);
  bool KeepFile(const std::string& fname);
};

// Backup dirs might contain .ldb files that do not belong to the snapshot
// they represent (but rather to a posterior one), i.e., they are not listed
// in the manifest. These files would be overwritten were the backup opened
// by leveldb. Since they are hardlinked, we must be careful not to create new
// files by doing fopen(name, "w"), which would do
// open(name, O_CREAT | O_TRUNC | O_WRONLY) and thus truncate the file and
// hence corrupt the original DB. Instead, we create files by:
// (1) creating a file with a temporary name   fnameXXXXXX
// (2) renaming it to  fname
// This way, the original file is left as-is.
Status BackupEnv::NewWritableFile(const std::string& fname,
                                  WritableFile** result) {
  Status s;
  int fd;
  std::string tmpname_ = fname + "XXXXXX";
  char tmpname[tmpname_.size() + 1];

  std::copy(tmpname_.begin(), tmpname_.end(), tmpname);
  tmpname[tmpname_.size()] = '\0';

  if((fd = mkstemp(tmpname)) < 0) {
    *result = NULL;
    s = IOError(fname, errno);
  }

  if(rename(tmpname, fname.c_str()) < 0) {
    *result = NULL;
    s = IOError(fname, errno);
  }

  FILE* f = fdopen(fd, "w");
  if (f == NULL) {
    close(fd);
    *result = NULL;
    s = IOError(fname, errno);
  } else {
    *result = new PosixWritableFile(fname, f);
  }
  return s;
}


Status BackupEnv::DeleteFile(const std::string& f) {
  mu_.Lock();
  Status s;
  if (backing_up_) {
    deferred_deletions_.push_back(f);
  } else {
    s = target()->DeleteFile(f);
  }
  mu_.Unlock();
  return s;
}

// Call (*save)(arg, filename, length) for every file that
// should be backed up to construct a consistent view of the
// database.  "length" may be negative to indicate that the entire
// file must be copied.  Otherwise the first "length" bytes must be
// copied.
Status
BackupEnv::Backup(const std::string& dir,
                  int (*func)(void*, const char* fname, uint64_t length),
                  void* arg) {
  // Get a consistent view of all files.
  mu_.Lock();
  std::vector<std::string> files;
  Status s = GetChildren(dir, &files);
  if (!s.ok()) {
    mu_.Unlock();
    return s;
  }
  std::vector<uint64_t> lengths(files.size());
  for (size_t i = 0; i < files.size(); i++) {
    if (files[i][0] == '.') {
      continue;
    }
    if (CopyFile(files[i])) {
      uint64_t len;
      s = GetFileSize(dir + "/" + files[i], &len);
      if (!s.ok()) {
        mu_.Unlock();
        return s;
      }
      lengths[i] = len;
    } else {
      lengths[i] = -1;
    }
  }
  backing_up_ = true;
  mu_.Unlock();

  for (size_t i = 0; s.ok() && i < files.size(); i++) {
    if (KeepFile(files[i])) {
      if ((*func)(arg, files[i].c_str(), lengths[i]) != 0) {
          s = Status::IOError("backup failed");
          break;
      }
    }
  }

  mu_.Lock();
  backing_up_ = false;
  for (size_t i = 0; i < deferred_deletions_.size(); i++) {
    target()->DeleteFile(deferred_deletions_[i]);
  }
  deferred_deletions_.clear();
  mu_.Unlock();

  return s;
}

// from leveldb util/logging.cc

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      const int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

// taken from leveldb   db/filename.{h,cc}

enum FileType {
  kLogFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile  // Either the current one, or an old one
};

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& fname,
                   uint64_t* number,
                   FileType* type) {
  Slice rest(fname);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}


bool BackupEnv::CopyFile(const std::string& fname) {
  uint64_t number;
  FileType type;
  ParseFileName(fname, &number, &type);
  return type != kTableFile;
}

bool BackupEnv::KeepFile(const std::string& fname) {
  uint64_t number;
  FileType type;
  if (ParseFileName(fname, &number, &type)) {
    switch (type) {
      case kLogFile:
      case kTableFile:
      case kDescriptorFile:
      case kCurrentFile:
      case kInfoLogFile:
        return true;
      case kDBLockFile:
      case kTempFile:
        return false;
    }
  }
  return false;
}

// vim: set sw=2 expandtab:
