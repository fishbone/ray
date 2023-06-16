#pragma once
#include <boost/asio.hpp>

#include "ray/util/status.h"

namespace ray {
namespace object_store {
class ExternalStorage {
 public:
  template <typename Token, typename ConstBuffer>
  auto AsyncWriteSome(std::string_view key, const ConstBuffer &buffer, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this, key, buffer = boost::asio::buffer(buffer.data(), buffer.size())](
            boost::asio::any_completion_handler<void(Status)> handle) {
          DoWrite(key, buffer, std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token, typename MutableBuffer>
  auto AsyncReadSome(std::string_view key, const MutableBuffer &buffer, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this,
         key,
         buffer = boost::asio::mutable_buffer(static_cast<void *>(buffer.data()),
                                              buffer.size())](
            boost::asio::any_completion_handler<void(Status)> handle) {
          DoRead(key, buffer, std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token>
  auto AsyncExists(std::string_view key, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this, key](boost::asio::any_completion_handler<void(Status)> handle) {
          DoExists(key, std::move(handle));
        },
        std::forward<Token>(token));
  }

  template <typename Token>
  auto AsyncDelete(std::string_view key, Token &&token) {
    return boost::asio::async_initiate<Token, void(Status)>(
        [this, key](boost::asio::any_completion_handler<void(Status)> handle) {
          DoDelete(key, std::move(handle));
        },
        std::forward<Token>(token));
  }

  virtual ~ExternalStorage() = default;

 protected:
  template <typename Signature, typename... Args>
  void ExecuteCallback(boost::asio::any_completion_handler<Signature> handle,
                       Args &&...args) {
    auto e = boost::asio::get_associated_executor(handle);
    RAY_CHECK(e) << "Invalid executor";
    boost::asio::dispatch(e, std::bind(std::move(handle), std::forward<Args>(args)...));
  }

  virtual void DoWrite(std::string_view key,
                       const boost::asio::const_buffer &data,
                       boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Write hasn't been implemented"));
  }

  virtual void DoRead(std::string_view key,
                      const boost::asio::mutable_buffer &buffer,
                      boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Read hasn't been implemented"));
  }

  virtual void DoExists(std::string_view key,
                        boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Exsits hasn't been implemented"));
  }

  virtual void DoDelete(std::string_view key,
                        boost::asio::any_completion_handler<void(Status)> handle) {
    ExecuteCallback(std::move(handle),
                    Status::NotImplemented("Delete hasn't been implemented"));
  }
};
}  // namespace object_store
}  // namespace ray