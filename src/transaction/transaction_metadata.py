from typing import Dict, List

class TransactionMetadata():
    def __init__(self, txn_id):
        self._txn_id = txn_id
        self._file_versions = {}
    
    def get_file_version(self, file_name: str) -> int:
        if file_name not in self._file_versions:
            return 0
        else:
            return self._file_versions[file_name]
    
    def increment_file_version(self, file_name: str) -> int:
        if file_name not in self._file_versions:
            self._file_versions[file_name] = 1
        else:
            self._file_versions[file_name] = self._file_versions[file_name] + 1
        return self._file_versions[file_name]
    
    def get_latest_file_versions(self) -> Dict[str, int]:
        file_versions = {}
        for key, value in self._file_versions.items():
            file_versions[key] = value-1
        return file_versions
    
    def get_updated_files(self) -> List[str]:
        file_urls = []
        for key in self._file_versions:
            file_urls.append(key)
        return file_urls