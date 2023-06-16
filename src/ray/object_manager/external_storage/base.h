#pragma once
#include <boost/asio.hpp>

#include "ray/util/status.h"

namespace ray {
namespace object_store {
class File {
 public:
  using FileFlags = boost::asio::file_base::flags;
  using SeekBasis = boost::asio::file_base::seek_basis;

  using MutableBuffer = boost::asio::mutable_buffer;
  using ConstBuffer = boost::asio::const_buffer;

  File() = default;

  virtual bool Open(const std::string &file, FileFlags open_flags) { return false; }
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
          DoRead(key, buffer, std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token>
  auto AsyncExists(Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this](boost::asio::any_completion_handler<void(Status)> handle) {
          DoExists(std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token>
  auto AsyncDelete(Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this](boost::asio::any_completion_handler<void(Status)> handle) {
          DoDelete(std::move(handle));
        },
        std::forward<Token>(token));
  }

  virtual uint64_t Seek(int64_t offset, asio::file_base::seek_basis whence) { return -1; }
  virtual int64_t Size() const { return -1; }

 protected:
  template <typename Signature, typename... Args>
  void ExecuteCallback(boost::asio::any_completion_handler<Signature> handle,
                       Args &&...args) {
    auto e = boost::asio::get_associated_executor(handle);
    RAY_CHECK(e) << "Invalid executor";
    boost::asio::dispatch(e, std::bind(std::move(handle), std::forward<Args>(args)...));
  }

  virtual void DoWrite(const boost::asio::const_buffer &data,
                       boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Write hasn't been implemented"));
  }

  virtual void DoRead(const boost::asio::mutable_buffer &buffer,
                      boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Read hasn't been implemented"));
  }

  virtual void DoExists(boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Exsits hasn't been implemented"));
  }

  virtual void DoDelete(boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Delete hasn't been implemented"));
  }
};
}  // namespace object_store
}  // namespace ray