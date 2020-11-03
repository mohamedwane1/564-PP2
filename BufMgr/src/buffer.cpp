/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	for(int i = 0; i < numBufs; i++){
		if(bufDescTable[i].dirty){
			flushFile(bufDescTable[i].file);
		}
	}
	delete [] bufPool;
	delete [] bufDescTable;
	
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	bool found = false;
	std::uint32_t i = 0;

	for(i = 0; i < numBufs; i++) {
		advanceClock();
		if (!bufDescTable[clockHand].valid) {
			found = true;
			break;
		}
		else if (bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false;
		}
		else if (bufDescTable[clockHand].pinCnt != 0){

		}
		else 
		{
			found = true;
			hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			if (bufDescTable[clockHand].dirty)
			{
				bufDescTable[clockHand].dirty = false;
				bufDescTable[clockHand].file->writePage(bufPool[clockHand]); 
			}
		}
	}
	if (!found && i >= numBufs) throw BufferExceededException();
	bufDescTable[clockHand].Clear();
	frame = clockHand;	
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::flushFile(const File* file) 
{
	for(std::uint32_t i = 0; i < numBufs; i++){
		if(bufDescTable[i].file == file && !bufDescTable[i].valid){
			throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
		}
		else if(bufDescTable[i].file == file && bufDescTable[i].valid){
			if(bufDescTable[i].pinCnt != 0){
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			if(bufDescTable[i].dirty){
				bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
		
	}
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
