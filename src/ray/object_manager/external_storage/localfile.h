#include "ray/object_manager/external_storage/base.h"
#include <fstream>

namespace ray {
namespace object_store {

class LocalFileManager : public FileManager {
public:
 std::unique_ptr<File> Open(const std::string &path, FileFlags flags) override;

private:
 void DoExists(const std::string &path,
               boost::asio::any_completion_handler<void(Status)> handle) override;
 void DoDelete(const std::string &path,
               boost::asio::any_completion_handler<void(Status)> handle) override;

 boost::asio::io_context io_context_;
};

class LocalFile : public File {
public:
 LocalFile(const std::string &path, FileFlags flags);

 uint64_t Seek(int64_t offset) override {
   return 0;
 }
 
 int64_t Size() const override {
  return 0;
 }
 
 ~LocalFile() override;

private:
  void DoWrite(const boost::asio::const_buffer& data,
                       boost::asio::any_completion_handler<void(Status)> handle) override;

  void DoRead(const boost::asio::mutable_buffer &buffer,
                      boost::asio::any_completion_handler<void(Status)> handle) override;

private:
  std::fstream file_;
};
}
}