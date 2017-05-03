class FileIterator:
    def __init__(self, fs, file_id, start):
        self.fs = fs
        self.start = start
        self.file_id = file_id

    def seek(self, position):
        self.start = position
