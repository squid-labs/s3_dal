# s3-dal
s3-dal stands for s3 distributed architecture log. This is an S3 distributed, durable and highly available log. 

# concept
This is an experimental code to test s3 write. This can be beneficial to application that wants to continue writing from the last crash where this can trace to the list of keys, finds the last inserted object. Here it is using CRC16 which is only 2 bytes + 8 bytes for the initial offset, in total it is 10 bytes. This is fast in terms of the logging speed as well, this uses less cpu and memory. This is unlike traditional logs which generally log in one whole log split by date or size. 