#include "gtest/gtest.h"
#include "ray/object_manager/external_storage/base.h"

TEST(ExternalStorageTest, Test) {
  ray::object_store::ExternalStorage store;
  ASSERT_TRUE(store.AsyncWriteSome("A", std::string("B"), boost::asio::use_future)
                  .get()
                  .IsNotImplemented());
  ASSERT_TRUE(store.AsyncReadSome("A", std::string(), boost::asio::use_future)
                  .get()
                  .IsNotImplemented());
}