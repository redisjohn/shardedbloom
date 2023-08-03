import redis 
import random
import hashlib
import datetime

#
#  Parameters to Set 
#
SHARDCOUNT = 4   #Total number of primary shards.  Can be 2,3 or 4
HOST = "127.0.0.1"
PORT = "6379"
PASSWORD="passwordhere"
PipeLineSize = 1000000
#
# Bloom Filter Parameters
#
TotalItemsAddedToFilters = 5000  #Total Estimated Items in all Bloom Filters
ErrorRate = .01
capacity = TotalItemsAddedToFilters//SHARDCOUNT

global r
global pipeline
checklist = []

#maps the first digit of the SHA256 to a bloom filter
Shard2Dict = {
    "3" : 3, 
    "7" : 3,
    "f" : 3,
    "b" : 3,
    "2" : 3,
    "6" : 3,
    "c" : 3,
    "1" : 0,   
    "5" : 0,
    "9" : 0,
    "d" : 0,
    "0" : 0,
    "4" : 0,   
    "8" : 0,
    "e" : 0,
    "a" : 0,    
}
Shard3Dict = {
    "0" : 0,
    "1" : 0,
    "2" : 0,
    "3" : 0, 
    "4" : 0,
    "5" : 0,
    "6" : 2,
    "7" : 2,
    "8" : 2,
    "9" : 2,
    "a" : 2,
    "b" : 3,
    "c" : 3,
    "d" : 3 ,
    "e" : 3 ,
    "f" : 3,
}
Shard4Dict = {
    "0" : 3, 
    "1" : 3,
    "2" : 3,
    "3" : 3,
    "4" : 2,
    "5" : 2,
    "6" : 2,
    "7" : 2,   
    "8" : 1,
    "9" : 1,
    "a" : 1,
    "b" : 1,
    "c" : 0,   
    "d" : 0,
    "e" : 0,
    "f" : 0,    
}


# Generaates a Random Sha-256
def GenSha256():
    random_bytes = bytearray(random.getrandbits(8) for _ in range(64 // 2))
    random_hex_string = ''.join(f'{x:02x}' for x in random_bytes)
    sha256_hash = hashlib.sha256(random_hex_string.encode()).hexdigest()
    return sha256_hash

# Map First Character of Key to Bloom Filter Number
def GetBloomFilterKey(firstchar):
        if (SHARDCOUNT == 2):
            return Shard2Dict[firstchar]
        elif (SHARDCOUNT == 3):
           return Shard3Dict[firstchar]
        elif (SHARDCOUNT == 4):
            return Shard4Dict[firstchar]

# Get BloomFilter associated with a Sha256
def MapSha256(key):
    bloomkey = str(GetBloomFilterKey(key[0]))
    keyname = "Bloom{" + bloomkey + "}"
    return keyname

def FlushDb():
    pool = redis.ConnectionPool(host=HOST,port=PORT,password=PASSWORD)  
    r = redis.Redis(connection_pool=pool)
    r.flushdb()       
    r.close

# Configure Bloom filter Error Rate and Initial Capacity
def ConfigureBloom(id):
        bloom = "Bloom" + "{" + str(id) + "}"
        r.bf().reserve(bloom,ErrorRate,capacity)

def SetupDB():
    global r 
    global pipeline
    #
    #  Setup connections for each pipeline and configure bloom filters
    # 
    pool = redis.ConnectionPool(host=HOST,port=PORT,password=PASSWORD)  
    r = redis.Redis(connection_pool=pool)
    pipeline = r.pipeline(transaction=False)   # transaction = false to avoid wrapping pipeline in Multi/Exec

    if SHARDCOUNT >= 2:           
        ConfigureBloom(3)
        ConfigureBloom(0)
    if SHARDCOUNT >= 3:
        ConfigureBloom(2)
    if SHARDCOUNT == 4:
        ConfigureBloom(1)

# Process Verification Pipeline
def VerifyResults():
    global pipeline 
    keyschecked = 0
    for sha255 in checklist:
        keyschecked = keyschecked + 1
        BloomKey = MapSha256(sha255[0])        
        pipeline.bf().exists(BloomKey,sha255)

    results = pipeline.execute()
    for result in results:
        if result == 0:
            print("Error False Negative")
            exit(-1)
    checklist.clear()

# Process Batches of Sha256
def ProcessShaBatch():
    for i in range(PipeLineSize):
        random_sha256_string = GenSha256()
        checklist.append(random_sha256_string)
        BloomKey = MapSha256(random_sha256_string)  
        Key=random_sha256_string
        pipeline.bf().add(BloomKey,Key)
    pipeline.execute()

#
#   Batch load random Sha256s into the database 
#   and verify they existence in their respective 
#   bloom filters
#
def LoadandVerifyBloomFilter():
    print("Start Loading:",datetime.datetime.now())
    global forceclose
    iterations = (TotalItemsAddedToFilters//PipeLineSize)
    for i in range(iterations): 
        ProcessShaBatch() 
        VerifyResults()    
    print("End Loading:",datetime.datetime.now())

#
#   Main
#   
if __name__ == '__main__':
    FlushDb()
    SetupDB()    
    LoadandVerifyBloomFilter()
        
 
   

