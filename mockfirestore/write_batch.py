from typing import Dict, Any
from mockfirestore.document import DocumentReference
from mockfirestore._helpers import get_by_path, set_by_path, delete_by_path
from mockfirestore.exceptions import NotFound


class WriteBatch:
    def __init__(self, mock_firestore):
        self._mock_firestore = mock_firestore
        self._operations = []

    def set(
        self,
        document_reference: DocumentReference,
        data: Dict[str, Any],
        merge: bool = False,
    ):
        self._operations.append(
            {"type": "set", "ref": document_reference, "data": data, "merge": merge}
        )
        return self

    def update(self, document_reference: DocumentReference, data: Dict[str, Any]):
        self._operations.append(
            {"type": "update", "ref": document_reference, "data": data}
        )
        return self

    def delete(self, document_reference: DocumentReference):
        self._operations.append({"type": "delete", "ref": document_reference})
        return self

    def commit(self):
        for operation in self._operations:
            if operation["type"] == "set":
                if operation["merge"]:
                    current_data = get_by_path(
                        self._mock_firestore._data, operation["ref"]._path
                    )
                    if current_data:
                        current_data.update(operation["data"])
                    else:
                        set_by_path(
                            self._mock_firestore._data,
                            operation["ref"]._path,
                            operation["data"],
                        )
                else:
                    set_by_path(
                        self._mock_firestore._data,
                        operation["ref"]._path,
                        operation["data"],
                    )
            elif operation["type"] == "update":
                current_data = get_by_path(
                    self._mock_firestore._data, operation["ref"]._path
                )
                if current_data:
                    current_data.update(operation["data"])
                else:
                    raise NotFound(
                        "No document to update: {}".format(operation["ref"]._path)
                    )
            elif operation["type"] == "delete":
                # Ensure the document is marked as non-existent
                set_by_path(self._mock_firestore._data, operation["ref"]._path, None)
        self._operations.clear()
