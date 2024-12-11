# s3-dal
s3-dal stands for s3 distributed architecture log. This is meant for AWS S3 which is distributed, durable and highly available log. 

# concept
This is an experimental code to test s3 write. This can be beneficial to application that wants to continue writing from the last crash where this can trace to the list of keys, finds the last inserted object. Here it is using CRC16 which is only 2 bytes + 8 bytes for the initial offset, in total it is 10 bytes. This is fast in terms of the logging speed as well, this uses less cpu and memory. This is unlike traditional logs which generally log in one whole log split by date or size. When a failure happens, it restarts from 0

# Addons
1. S3 support (done)
2. CRC16 (done)
3. File extension size check (done)
4. ORC support
5. Compression gzip(Plan)
7. Revisit different file type support
8. Revisit other cloud provider
9. Refactoring
10. New Algo for s3 search 


# Limitation
S3’s limitations:
1. S3 has no API to fetch the last inserted item.
2. The LIST API doesn’t support sorting; it always returns results in lexicographical order. For now i am using this. However hoping that there will be a combination in the future for filetypes or compaction that ride on this method