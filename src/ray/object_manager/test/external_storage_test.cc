#include "gtest/gtest.h"
#include "ray/object_manager/external_storage/base.h"

TEST(ExternalStorageTest, Test) {
  ray::object_store::File file;
  ASSERT_TRUE(file.AsyncWriteSome("A", std::string("B"), boost::asio::use_future)
                  .get()
                  .IsNotImplemented());
  ASSERT_TRUE(file.AsyncReadSome("A", std::string(), boost::asio::use_future)
                  .get()
                  .IsNotImplemented());
}