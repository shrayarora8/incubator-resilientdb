/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "executor/kv/kv_executor.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "chain/storage/memory_db.h"
#include "chain/storage/storage.h"
#include "common/test/test_macros.h"
#include "platform/config/resdb_config_utils.h"
#include "proto/kv/kv.pb.h"

namespace resdb {
namespace {

using ::resdb::testing::EqualsProto;
using storage::MemoryDB;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::Test;

class KVExecutorTest : public Test {
 public:
  KVExecutorTest() {
    auto storage = std::make_unique<MemoryDB>();
    storage_ptr_ = storage.get();
    impl_ = std::make_unique<KVExecutor>(std::move(storage));
  }

  int Set(const std::string& key, const std::string& value) {
    KVRequest request;
    request.set_cmd(KVRequest::SET);
    request.set_key(key);
    request.set_value(value);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return -1;
    }

    impl_->ExecuteData(str);
    return 0;
  }

  std::string Get(const std::string& key) {
    KVRequest request;
    request.set_cmd(KVRequest::GET);
    request.set_key(key);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return "";
    }
    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return "";
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return "";
    }
    return kv_response.value();
  }

  std::string GetAllValues() {
    KVRequest request;
    request.set_cmd(KVRequest::GETALLVALUES);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return "";
    }
    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return "";
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return "";
    }
    return kv_response.value();
  }

  std::string GetRange(const std::string& min_key, const std::string& max_key) {
    KVRequest request;
    request.set_cmd(KVRequest::GETRANGE);
    request.set_key(min_key);
    request.set_value(max_key);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return "";
    }
    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return "";
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return "";
    }
    return kv_response.value();
  }

  int Set(const std::string& key, const std::string& value, int version) {
    KVRequest request;
    request.set_cmd(KVRequest::SET_WITH_VERSION);
    request.set_key(key);
    request.set_value(value);
    request.set_version(version);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return -1;
    }

    impl_->ExecuteData(str);
    return 0;
  }

  ValueInfo Get(const std::string& key, int version) {
    KVRequest request;
    request.set_cmd(KVRequest::GET_WITH_VERSION);
    request.set_key(key);
    request.set_version(version);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return ValueInfo();
    }

    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return ValueInfo();
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return ValueInfo();
    }

    return kv_response.value_info();
  }

  Items GetAllItems() {
    KVRequest request;
    request.set_cmd(KVRequest::GET_ALL_ITEMS);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return Items();
    }

    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return Items();
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return Items();
    }

    return kv_response.items();
  }

  Items GetKeyRange(const std::string& min_key, const std::string& max_key) {
    KVRequest request;
    request.set_cmd(KVRequest::GET_KEY_RANGE);
    request.set_min_key(min_key);
    request.set_max_key(max_key);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return Items();
    }

    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return Items();
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return Items();
    }

    return kv_response.items();
  }

  Items GetHistory(const std::string& key, int min_version, int max_version) {
    KVRequest request;
    request.set_cmd(KVRequest::GET_HISTORY);
    request.set_key(key);
    request.set_min_version(min_version);
    request.set_max_version(max_version);

    std::string str;
    if (!request.SerializeToString(&str)) {
      return Items();
    }

    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return Items();
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return Items();
    }

    return kv_response.items();
  }

  void CreateCompositeKey(const std::string& composite_key,
                          const std::string& primary_key) {
    KVRequest request;
    request.set_cmd(KVRequest::CREATE_COMPOSITE_KEY);
    request.set_composite_key(composite_key);
    request.set_primary_key(primary_key);
    std::string str;
    request.SerializeToString(&str);
    impl_->ExecuteData(str);
  }

  void DeleteCompositeKey(const std::string& composite_key) {
    KVRequest request;
    request.set_cmd(KVRequest::DELETE_COMPOSITE_KEY);
    request.set_composite_key(composite_key);
    std::string str;
    request.SerializeToString(&str);
    impl_->ExecuteData(str);
  }

  void UpdateCompositeKey(const std::string& old_composite_key,
                          const std::string& new_composite_key) {
    KVRequest request;
    request.set_cmd(KVRequest::UPDATE_COMPOSITE_KEY);
    request.set_old_composite_key(old_composite_key);
    request.set_new_composite_key(new_composite_key);
    std::string str;
    request.SerializeToString(&str);
    impl_->ExecuteData(str);
  }

  Items GetByCompositeKeyPrefix(const std::string& prefix) {
    KVRequest request;
    request.set_cmd(KVRequest::GET_BY_COMPOSITE_KEY_PREFIX);
    request.set_composite_key_prefix(prefix);
    std::string str;
    if (!request.SerializeToString(&str)) {
      return Items();
    }
    auto resp = impl_->ExecuteData(str);
    if (resp == nullptr) {
      return Items();
    }
    KVResponse kv_response;
    if (!kv_response.ParseFromString(*resp)) {
      return Items();
    }
    return kv_response.items();
  }

 protected:
  Storage* storage_ptr_;

 private:
  std::unique_ptr<KVExecutor> impl_;
};

TEST_F(KVExecutorTest, SetValue) {
  std::map<std::string, std::string> data;

  EXPECT_EQ(GetAllValues(), "[]");
  EXPECT_EQ(Set("test_key", "test_value"), 0);
  EXPECT_EQ(Get("test_key"), "test_value");

  // GetAllValues and GetRange may be out of order for in-memory, so we test up
  // to 1 key-value pair
  EXPECT_EQ(GetAllValues(), "[test_value]");
  EXPECT_EQ(GetRange("a", "z"), "[test_value]");
}

TEST_F(KVExecutorTest, SetValueWithVersion) {
  std::map<std::string, std::string> data;

  {
    EXPECT_EQ(Set("test_key", "test_value", 0), 0);
    ValueInfo expected_info;
    expected_info.set_value("test_value");
    expected_info.set_version(1);
    EXPECT_THAT(Get("test_key", 1), EqualsProto(expected_info));
  }

  {
    EXPECT_EQ(Set("test_key", "test_value1", 1), 0);
    ValueInfo expected_info;
    expected_info.set_value("test_value1");
    expected_info.set_version(2);
    EXPECT_THAT(Get("test_key", 2), EqualsProto(expected_info));
  }
  {
    EXPECT_EQ(Set("test_key", "test_value1", 1), 0);
    ValueInfo expected_info;
    expected_info.set_value("test_value");
    expected_info.set_version(1);
    EXPECT_THAT(Get("test_key", 1), EqualsProto(expected_info));
  }

  {
    EXPECT_EQ(Set("test_key1", "test_key1", 0), 0);
    ValueInfo expected_info;
    expected_info.set_value("test_key1");
    expected_info.set_version(1);
    EXPECT_THAT(Get("test_key1", 1), EqualsProto(expected_info));

    ValueInfo expected_info2;
    expected_info2.set_value("test_value");
    expected_info2.set_version(1);
    EXPECT_THAT(Get("test_key", 1), EqualsProto(expected_info2));

    ValueInfo expected_info3;
    expected_info3.set_value("test_value1");
    expected_info3.set_version(2);
    EXPECT_THAT(Get("test_key", 0), EqualsProto(expected_info3));
  }

  {
    Items items;
    {
      Item* item = items.add_item();
      item->set_key("test_key");
      item->mutable_value_info()->set_value("test_value1");
      item->mutable_value_info()->set_version(2);
    }
    {
      Item* item = items.add_item();
      item->set_key("test_key1");
      item->mutable_value_info()->set_value("test_key1");
      item->mutable_value_info()->set_version(1);
    }

    EXPECT_THAT(GetAllItems(), EqualsProto(items));
  }

  {
    Items items;
    {
      Item* item = items.add_item();
      item->set_key("test_key");
      item->mutable_value_info()->set_value("test_value1");
      item->mutable_value_info()->set_version(2);
    }
    EXPECT_THAT(GetKeyRange("test_key", "test_key"), EqualsProto(items));
  }

  {
    Items items;
    {
      Item* item = items.add_item();
      item->set_key("test_key");
      item->mutable_value_info()->set_value("test_value1");
      item->mutable_value_info()->set_version(2);
    }
    {
      Item* item = items.add_item();
      item->set_key("test_key");
      item->mutable_value_info()->set_value("test_value");
      item->mutable_value_info()->set_version(1);
    }
    EXPECT_THAT(GetHistory("test_key", 0, 2), EqualsProto(items));
  }
}

TEST_F(KVExecutorTest, CreateCompositeKey_HappyPath) {
  // Primary key must exist before the executor will allow indexing.
  ASSERT_EQ(Set("user_42", "alice"), 0);

  CreateCompositeKey("ck1", "user_42");

  Items result = GetByCompositeKeyPrefix("ck1");
  ASSERT_EQ(result.item_size(), 1);
  EXPECT_EQ(result.item(0).key(), "ck1");
}

TEST_F(KVExecutorTest, CreateCompositeKey_RejectsOrphan) {
  // No primary key was set: the executor must refuse to index.
  CreateCompositeKey("ck_orphan", "nonexistent_pk");

  Items result = GetByCompositeKeyPrefix("ck_orphan");
  EXPECT_EQ(result.item_size(), 0);
}

TEST_F(KVExecutorTest, DeleteCompositeKey_RemovesEntry) {
  ASSERT_EQ(Set("user_42", "alice"), 0);
  CreateCompositeKey("ck1", "user_42");
  ASSERT_EQ(GetByCompositeKeyPrefix("ck1").item_size(), 1);

  DeleteCompositeKey("ck1");
  EXPECT_EQ(GetByCompositeKeyPrefix("ck1").item_size(), 0);
}

TEST_F(KVExecutorTest, UpdateCompositeKey_MovesEntry) {
  ASSERT_EQ(Set("user_42", "alice"), 0);
  CreateCompositeKey("ck_old", "user_42");

  UpdateCompositeKey("ck_old", "ck_new");

  EXPECT_EQ(GetByCompositeKeyPrefix("ck_old").item_size(), 0);
  Items new_result = GetByCompositeKeyPrefix("ck_new");
  ASSERT_EQ(new_result.item_size(), 1);
  EXPECT_EQ(new_result.item(0).key(), "ck_new");
}

TEST_F(KVExecutorTest, GetByCompositeKeyPrefix_MultipleMatches) {
  ASSERT_EQ(Set("user_1", "alice"), 0);
  ASSERT_EQ(Set("user_2", "bob"), 0);

  CreateCompositeKey("idx_a", "user_1");
  CreateCompositeKey("idx_b", "user_2");
  CreateCompositeKey("other_key", "user_1");

  Items result = GetByCompositeKeyPrefix("idx_");
  ASSERT_EQ(result.item_size(), 2);
  EXPECT_EQ(result.item(0).key(), "idx_a");
  EXPECT_EQ(result.item(1).key(), "idx_b");
}

}  // namespace

}  // namespace resdb
