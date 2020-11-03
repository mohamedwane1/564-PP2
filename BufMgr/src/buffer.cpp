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
 #include <stdint.h>

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
}

void BufMgr::advanceClock()
{
	clockHand++;
	clockHand %= numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	bool found = false;
	std::uint32_t i = 0;
	for(i = 0; i < numBufs; i++) 
	{
		advanceClock();
		if (!bufDescTable[clockHand].valid) {
			found = true;
			break;
		}
			
		else if (bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false;

		}
		else if (bufDescTable[clockHand].pinCnt == 0) {
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
	FrameId frameNum;
	try
	{
		hashTable->lookup(file, pageNo, frameNum);
		bufDescTable[frameNum].refbit = true;
		bufDescTable[frameNum].pinCnt += 1;
	}
	catch(HashNotFoundException e)
	{
		allocBuf(frameNum); 
		bufPool[frameNum] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNum);
		bufDescTable[frameNum].Set(file, pageNo);
	}
	
	page = &bufPool[frameNum];

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNum;
	allocBuf(frameNum);
	bufPool[frameNum] = file->allocatePage();
	pageNo = bufPool[frameNum].page_number();
	hashTable->insert(file, pageNo, frameNum);
	bufDescTable[frameNum].Set(file, pageNo);
	page = &bufPool[frameNum];
}

void BufMgr::flushFile(const File* file) 
{
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