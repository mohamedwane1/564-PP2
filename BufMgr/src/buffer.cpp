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

/*
* Go through the bufDesc table and if the page is dirty, flush the page by calling
* flushFile(). After flushing the dirty files, deallocate the buffer pool and bufDescTable
*/
BufMgr::~BufMgr() {
	for(int i = 0; i < numBufs; i++){
		if(bufDescTable[i].dirty){
			flushFile(bufDescTable[i].file);
		}
	}
	delete [] bufPool;
	delete [] bufDescTable;
	
}

//Advance the clock to the next frame
void BufMgr::advanceClock()
{
	clockHand++;
	clockHand %= numBufs;
}


void BufMgr::allocBuf(FrameId & frame) 
{
	bool found = false;
	std::uint32_t i = 0;

	for(i = 0; i <= numBufs; i++) {
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
			break;
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
	FrameId frameNum;
    try {
        hashTable->lookup(file, pageNo, frameNum);
        if (bufDescTable[frameNum].pinCnt == 0) 
            throw PageNotPinnedException(file->filename(), pageNo, frameNum);
        bufDescTable[frameNum].pinCnt--;
        if(dirty) bufDescTable[frameNum].dirty = true;
    }
    catch(HashNotFoundException e)
    {

    }
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

/*
* Scans bufDescTable for pages in a file. We throw a BadBufferException if an 
* invalid page in a file is found. If the page in the file is valid but the pincount
* does not equal 0, we throw a pagePinnedException. If the page is valid and in the file and
* the page is dirty, we cal writePage to flush the page to the disk and set the dirtbit to false.
* We then remove the page from the hashtable and clear the bufdesc for the page frame.
*
*/
void BufMgr::flushFile(const File* file) 
{
	for(std::uint32_t i = 0; i < numBufs; i++){
		//if the page is in a file but invalid, throw exception
		if(bufDescTable[i].file == file && !bufDescTable[i].valid){
			throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
		}
		else if(bufDescTable[i].file == file && bufDescTable[i].valid){
			//if the page in the file is pinned, throw exception
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
	//Check for page in the buffer pool
	FrameId frameNum;
	try {
		hashTable->lookup(file, PageNo, frameNum);
		//if page is found delete it & write if necessary
		if (bufDescTable[frameNum].dirty) {
			bufDescTable[frameNum].dirty = false;
			file->writePage(bufPool[frameNum]);
		}
		//clear the frame
		bufDescTable[frameNum].Clear();
		hashTable->remove(file, PageNo);
		file->deletePage(PageNo);
	} catch (HashNotFoundException e) {
	file->deletePage(PageNo);
	}

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