import unittest
from mockfirestore import MockFirestore


class TestMockFirestoreBatch(unittest.TestCase):
    def setUp(self):
        self.fs = MockFirestore()

    def test_batch_set(self):
        batch = self.fs.batch()
        doc_ref = self.fs.collection("users").document("user1")
        batch.set(doc_ref, {"name": "John Doe", "age": 30})
        batch.commit()

        doc = doc_ref.get()
        self.assertEqual(doc.to_dict(), {"name": "John Doe", "age": 30})

    def test_batch_update(self):
        doc_ref = self.fs.collection("users").document("user2")
        doc_ref.set({"name": "Jane Doe", "age": 25})

        batch = self.fs.batch()
        batch.update(doc_ref, {"age": 26, "city": "New York"})
        batch.commit()

        doc = doc_ref.get()
        self.assertEqual(
            doc.to_dict(), {"name": "Jane Doe", "age": 26, "city": "New York"}
        )

    def test_batch_delete(self):
        doc_ref = self.fs.collection("users").document("user3")
        doc_ref.set({"name": "Bob Smith"})

        batch = self.fs.batch()
        batch.delete(doc_ref)
        batch.commit()

        doc = doc_ref.get()
        self.assertEqual(doc.to_dict(), None)

    def test_batch_mixed_operations(self):
        batch = self.fs.batch()

        self.fs._data = {
            "users": {
                "user5": {"name": "Bob", "age": 40},
                "user6": {"name": "Danny", "age": 10},
            }
        }

        doc1_ref = self.fs.collection("users").document("user4")
        doc2_ref = self.fs.collection("users").document("user5")
        doc3_ref = self.fs.collection("users").document("user6")

        doc3_ref.set({"name": "Charlie Brown"})

        batch.set(doc1_ref, {"name": "Alice", "age": 30})
        batch.update(doc2_ref, {"city": "London"})
        batch.delete(doc3_ref)

        batch.commit()

        self.assertEqual(doc1_ref.get().to_dict(), {"name": "Alice", "age": 30})
        self.assertEqual(
            doc2_ref.get().to_dict(), {"name": "Bob", "age": 40, "city": "London"}
        )
        self.assertEqual(doc3_ref.get().to_dict(), None)

    def test_batch_commit_method(self):
        batch = self.fs.batch()
        doc_ref = self.fs.collection("users").document("user7")
        batch.set(doc_ref, {"name": "Eve", "age": 28})

        MockFirestore.batch_commit(batch)

        doc = doc_ref.get()
        self.assertEqual(doc.to_dict(), {"name": "Eve", "age": 28})

    def test_multiple_batches(self):
        batch1 = self.fs.batch()
        batch2 = self.fs.batch()

        doc1_ref = self.fs.collection("users").document("user8")
        doc2_ref = self.fs.collection("users").document("user9")

        batch1.set(doc1_ref, {"name": "Frank"})
        batch2.set(doc2_ref, {"name": "Grace"})

        batch1.commit()
        batch2.commit()

        self.assertEqual(doc1_ref.get().to_dict(), {"name": "Frank"})
        self.assertEqual(doc2_ref.get().to_dict(), {"name": "Grace"})


if __name__ == "__main__":
    unittest.main()
