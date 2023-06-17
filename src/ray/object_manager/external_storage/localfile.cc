#include "ray/object_manager/external_storage/localfile.h"
#include <filesystem>

namespace ray {
namespace object_store {

void LocalFileManager::DoExists(
  const std::string& file,
  boost::asio::any_completion_handler<void(Status)> handle) {
  bool exists = std::filesystem::exists(file);
  Dispatch(std::move(handle),
           exists ? Status::OK() : Status::NotFound("File does not exist."));
}

void LocalFileManager::DoDelete(
  const std::string& file,
  boost::asio::any_completion_handler<void(Status)> handle) {
  std::error_code ec;
  if (!std::filesystem::remove(file, ec)) {
    Dispatch(std::move(handle), Status::IOError(ec.message()));
  }
  else {
    Dispatch(std::move(handle), Status::OK());
  }
}

std::unique_ptr<File> LocalFileManager::Open(const std::string &path,
                                                  FileFlags flags) {
  return std::make_unique<LocalFile>(path, flags);
}

LocalFile::LocalFile(const std::string &path, FileFlags flags) {
  std::ios::openmode mode;
  switch (flags) {
  case FileFlags::READ:
    mode = std::ios::in;
    break;
  case FileFlags::WRITE:
    mode = std::ios::out;
    break;
  default:
    RAY_LOG(FATAL) << "Invalid file flags.";
  }
  file_.open(path.c_str(), mode);
}

void LocalFile::DoWrite(const boost::asio::const_buffer& data,
                   boost::asio::any_completion_handler<void(Status)> handle) {
}

void LocalFile::DoRead(const boost::asio::mutable_buffer& buffer,
                  boost::asio::any_completion_handler<void(Status)> handle) {
}

}
}
