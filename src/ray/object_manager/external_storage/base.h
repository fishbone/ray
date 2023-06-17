#pragma once
#include <boost/asio.hpp>

#include "ray/util/status.h"

namespace ray {
namespace object_store {

namespace {
template<typename CompletionToken, typename ... Args>
auto Dispatch(CompletionToken&& token, Args&&...args) {
  auto e = boost::asio::get_associated_executor(token);
  RAY_CHECK(e) << "Invalid executor";
  boost::asio::dispatch(e, std::bind(std::forward<CompletionToken>(token), std::forward<Args>(args)...));
}
}

enum struct FileFlags { READ = 0, WRITE = 1 };

class File;

class FileManager {
 public:
  virtual ~FileManager() = default;

  virtual std::unique_ptr<File> Open(const std::string &file, FileFlags flags) = 0;

  template <typename Token>
  auto AsyncExists(const std::string& file, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this, file](boost::asio::any_completion_handler<void(Status)> handle) {
          DoExists(file, std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token>
  auto AsyncDelete(const std::string &file, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this, file](boost::asio::any_completion_handler<void(Status)> handle) {
          DoDelete(file, std::move(handle));
        },
        std::forward<Token>(token));
  }

 private:
  virtual void DoExists(
    const std::string& file, boost::asio::any_completion_handler<void(Status)> handle) = 0;
  virtual void DoDelete(
    const std::string& file, boost::asio::any_completion_handler<void(Status)> handle) = 0;
};

class File {
public:
  using MutableBuffer = boost::asio::mutable_buffer;
  using ConstBuffer = boost::asio::const_buffer;
  virtual ~File() = default;
  File() = default;
  virtual uint64_t Seek(int64_t offset) = 0;
  virtual int64_t Size() const = 0;

  template <typename Token, typename Buffer>
  auto AsyncWriteSome(const Buffer &buffer, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this, buffer = boost::asio::buffer(buffer.data(), buffer.size())](
            boost::asio::any_completion_handler<void(Status)> handle) {
          DoWrite(buffer, std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token, typename Buffer>
  auto AsyncReadSome(const Buffer &buffer, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this,
         buffer = boost::asio::mutable_buffer((void *)(buffer.data()), buffer.size())](
            boost::asio::any_completion_handler<void(Status)> handle) {
          DoRead(buffer, std::move(handle));
        },
        std::forward<Token>(token));
  }
private:
  virtual void DoWrite(const boost::asio::const_buffer &data,
                       boost::asio::any_completion_handler<void(Status)> handle) = 0;

  virtual void DoRead(const boost::asio::mutable_buffer &buffer,
                      boost::asio::any_completion_handler<void(Status)> handle) = 0;
};

}  // namespace object_store
}  // namespace ray
