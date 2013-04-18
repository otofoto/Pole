/* POLE - Portable C++ library to access OLE Storage 
   Copyright (C) 2002-2005 Ariya Hidayat <ariya@kde.org>

   Performance optimization: Dmitry Fedorov 
   Copyright 2009 <www.bioimage.ucsb.edu> <www.dimin.net> 

   Fix for more than 236 mbat block entries : Michel Boudinot
   Copyright 2010 <Michel.Boudinot@inaf.cnrs-gif.fr>

   Considerable rework to allow for creation and updating of structured storage : Stephen Baum
   Copyright 2013 <srbaum@gmail.com>

   Version: 0.5

   Redistribution and use in source and binary forms, with or without 
   modification, are permitted provided that the following conditions 
   are met:
   * Redistributions of source code must retain the above copyright notice, 
     this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above copyright notice, 
     this list of conditions and the following disclaimer in the documentation 
     and/or other materials provided with the distribution.
   * Neither the name of the authors nor the names of its contributors may be 
     used to endorse or promote products derived from this software without 
     specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
   AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
   ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
   LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
   CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
   SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
   INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
   CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
   ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
   THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <fstream>
#include <iostream>
#include <list>
#include <string>
#include <vector>

#include <cstring>

#include "pole.h"

// enable to activate debugging output
// #define POLE_DEBUG

namespace POLE
{

class Header
{
  public:
    unsigned char id[8];       // signature, or magic identifier
    unsigned int b_shift;          // bbat->blockSize = 1 << b_shift
    unsigned int s_shift;          // sbat->blockSize = 1 << s_shift
    unsigned int num_bat;          // blocks allocated for big bat
    unsigned int dirent_start;     // starting block for directory info
    unsigned int threshold;        // switch from small to big file (usually 4K)
    unsigned int sbat_start;       // starting block index to store small bat
    unsigned int num_sbat;         // blocks allocated for small bat
    unsigned int mbat_start;       // starting block to store meta bat
    unsigned int num_mbat;         // blocks allocated for meta bat
    unsigned int bb_blocks[109];
    bool dirty;                // Needs to be written
    
    Header();
    bool valid();
    void load( const unsigned char* buffer );
    void save( unsigned char* buffer );
    void debug();
};

class AllocTable
{
  public:
    static const unsigned int Eof;
    static const unsigned int Avail;
    static const unsigned int Bat;
    static const unsigned int MetaBat;
    unsigned blockSize;
    AllocTable();
    void clear();
    unsigned int count();
    unsigned int unusedCount();
    void resize( unsigned int newsize );
    void preserve( unsigned int n );
    void set( unsigned int index, unsigned int val );
    unsigned unused();
    void setChain( std::vector<unsigned int> );
    std::vector<unsigned int> follow( unsigned int start );
    unsigned int operator[](unsigned int index );
    void load( const unsigned char* buffer, unsigned int len );
    void save( unsigned char* buffer );
    unsigned size();
    void debug();
    bool isDirty();
    void markAsDirty(unsigned int dataIndex, int bigBlockSize);
    void flush(std::vector<unsigned int> blocks, StorageIO *const io, int bigBlockSize);
  private:
    std::vector<unsigned int> data;
    std::vector<unsigned int> dirtyBlocks;
    bool bMaybeFragmented;
    AllocTable( const AllocTable& );
    AllocTable& operator=( const AllocTable& );
};

class DirEntry
{
  public:
    bool valid;            // false if invalid (should be skipped)
    std::string name;      // the name, not in unicode anymore 
    bool dir;              // true if directory   
    unsigned int size;    // size (not valid if directory)
    unsigned int start;   // starting block
    unsigned prev;         // previous sibling
    unsigned next;         // next sibling
    unsigned child;        // first child
    int compare(const DirEntry& de);
    int compare(const std::string& name2);

};

class DirTree
{
  public:
    static const unsigned End;
    DirTree(int bigBlockSize);
    void clear(int bigBlockSize);
    inline unsigned int entryCount();
    unsigned int unusedEntryCount();
    DirEntry* entry( unsigned index );
    DirEntry* entry( const std::string& name, bool create = false, int bigBlockSize = 0, StorageIO *const io = 0, int streamSize = 0);
    int indexOf( DirEntry* e );
    int parent( unsigned index );
    std::string fullName( unsigned index );
    std::vector<unsigned> children( unsigned index );
    unsigned find_child( unsigned index, const std::string& name, unsigned &closest );
    void load( unsigned char* buffer, unsigned len );
    void save( unsigned char* buffer );
    unsigned size();
    void debug();
    bool isDirty();
    void markAsDirty(unsigned int dataIndex, int bigBlockSize);
    void flush(std::vector<unsigned int> blocks, StorageIO *const io, int bigBlockSize, unsigned int sb_start, unsigned int sb_size);
    unsigned int unused();
    void findParentAndSib(unsigned int inIdx, const std::string& inFullName, unsigned int &parentIdx, unsigned int &sibIdx);
    unsigned findSib(unsigned int inIdx, unsigned int sibIdx);
    void deleteEntry(DirEntry *entry, const std::string& inFullName, int bigBlockSize);
  private:
    std::vector<DirEntry> entries;
    std::vector<unsigned int> dirtyBlocks;
    DirTree( const DirTree& );
    DirTree& operator=( const DirTree& );
};

class StorageIO
{
  public:
    Storage* storage;         // owner
    std::string filename;     // filename
    std::fstream file;        // associated with above name
    int result;               // result of operation
    bool opened;              // true if file is opened
    unsigned long filesize;   // size of the file
    bool writeable;           // true if the file can be modified
    
    Header* header;           // storage header 
    DirTree* dirtree;         // directory tree
    AllocTable* bbat;         // allocation table for big blocks
    AllocTable* sbat;         // allocation table for small blocks
    
    std::vector<unsigned int> sb_blocks; // blocks for "small" files
    std::vector<unsigned int> mbat_blocks; // blocks for doubly indirect indices to big blocks
    std::vector<unsigned int> mbat_data; // the additional indices to big blocks
    bool mbatDirty;           // If true, mbat_blocks need to be written
       
    std::list<Stream*> streams;

    StorageIO( Storage* storage, const char* filename );
    ~StorageIO();
    
    bool open(bool bWriteAccess = false, bool bCreate = false);
    void close();
    void flush();
    void load(bool bWriteAccess);
    void create();
    void init();
    bool deleteByName(const std::string& fullName);

    bool deleteNode(DirEntry *entry, const std::string& fullName);

    bool deleteLeaf(DirEntry *entry, const std::string& fullName);

    unsigned int loadBigBlocks( std::vector<unsigned int> blocks, unsigned char* buffer, unsigned int maxlen );

    unsigned int loadBigBlock( unsigned int block, unsigned char* buffer, unsigned int maxlen );

    unsigned int saveBigBlocks( std::vector<unsigned int> blocks, unsigned int offset, unsigned char* buffer, unsigned int len );

    unsigned int saveBigBlock( unsigned int block, unsigned int offset, unsigned char*buffer, unsigned int len );

    unsigned int loadSmallBlocks( std::vector<unsigned int> blocks, unsigned char* buffer, unsigned int maxlen );

    unsigned int loadSmallBlock( unsigned int block, unsigned char* buffer, unsigned int maxlen );
    
    unsigned int saveSmallBlocks( std::vector<unsigned int> blocks, unsigned int offset, unsigned char* buffer, unsigned int len, int startAtBlock = 0  );

    unsigned int saveSmallBlock( unsigned int block, unsigned int offset, unsigned char* buffer, unsigned int len );
    
    StreamIO* streamIO( const std::string& name, bool bCreate = false, int streamSize = 0 ); 

    void flushbbat();

    void flushsbat();

    std::vector<unsigned int> getbbatBlocks(bool bLoading);

    unsigned int ExtendFile( std::vector<unsigned int> *chain );

    void addbbatBlock();

  private:  
    // no copy or assign
    StorageIO( const StorageIO& );
    StorageIO& operator=( const StorageIO& );

};

class StreamIO
{
  public:
    StorageIO* io;
    int entryIdx; //needed because a pointer to DirEntry will change whenever entries vector changes.
    std::string fullName;
    bool eof;
    bool fail;

    StreamIO( StorageIO* io, DirEntry* entry );
    ~StreamIO();
    unsigned int size();
    void setSize(unsigned int newSize);
    void seek( unsigned int pos );
    unsigned int tell();
    int getch();
    unsigned int read( unsigned char* data, unsigned int maxlen );
    unsigned int read( unsigned int pos, unsigned char* data, unsigned int maxlen );
    unsigned int write( unsigned char* data, unsigned int len );
    unsigned int write( unsigned int pos, unsigned char* data, unsigned int len );
    void flush();

  private:
    std::vector<unsigned int> blocks;

    // no copy or assign
    StreamIO( const StreamIO& );
    StreamIO& operator=( const StreamIO& );

    // pointer for read
    unsigned int m_pos;

    // simple cache system to speed-up getch()
    unsigned char* cache_data;
    unsigned int cache_size;
    unsigned int cache_pos;
    void updateCache();
#define CACHEBUFSIZE 4096 //a presumably reasonable sie for the read cache
};

}; // namespace POLE

using namespace POLE;

static void fileCheck(std::fstream &file)
{
    bool bGood, bFail, bEof, bBad;
    bool bNOTOK;
    bGood = file.good();
    bFail = file.fail();
    bEof = file.eof();
    bBad = file.bad();
    if (bFail || bEof || bBad)
        bNOTOK = true; //this doesn't really do anything, but it is a good place to set a breakpoint!
    file.clear();
}
    

static inline unsigned int readU16( const unsigned char* ptr )
{
  return ptr[0]+(ptr[1]<<8);
}

static inline unsigned int readU32( const unsigned char* ptr )
{
  return ptr[0]+(ptr[1]<<8)+(ptr[2]<<16)+(ptr[3]<<24);
}

static inline void writeU16( unsigned char* ptr, unsigned int data )
{
  ptr[0] = (unsigned char)(data & 0xff);
  ptr[1] = (unsigned char)((data >> 8) & 0xff);
}

static inline void writeU32( unsigned char* ptr, unsigned int data )
{
  ptr[0] = (unsigned char)(data & 0xff);
  ptr[1] = (unsigned char)((data >> 8) & 0xff);
  ptr[2] = (unsigned char)((data >> 16) & 0xff);
  ptr[3] = (unsigned char)((data >> 24) & 0xff);
}

static const unsigned char pole_magic[] = 
 { 0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1 };

// =========== Header ==========

Header::Header()
{
  b_shift = 9;      // [1EH,02] size of sectors in power-of-two; typically 9 indicating 512-byte sectors
  s_shift = 6;      // [20H,02] size of mini-sectors in power-of-two; typically 6 indicating 64-byte mini-sectors
  num_bat = 0;      // [2CH,04] number of SECTs in the FAT chain
  dirent_start = 0; // [30H,04] first SECT in the directory chain
  threshold = 4096; // [38H,04] maximum size for a mini stream; typically 4096 bytes
  sbat_start = 0;   // [3CH,04] first SECT in the MiniFAT chain
  num_sbat = 0;     // [40H,04] number of SECTs in the MiniFAT chain
  mbat_start = AllocTable::Eof;   // [44H,04] first SECT in the DIFAT chain
  num_mbat = 0;     // [48H,04] number of SECTs in the DIFAT chain

  for( unsigned i = 0; i < 8; i++ )
    id[i] = pole_magic[i];  
  for( unsigned i=0; i<109; i++ )
    bb_blocks[i] = AllocTable::Avail;
  dirty = true;
}

bool Header::valid()
{
  if( threshold != 4096 ) return false;
  if( num_bat == 0 ) return false;
  //if( (num_bat > 109) && (num_bat > (num_mbat * 127) + 109)) return false; // dima: incorrect check, number may be arbitrary larger
  if( (num_bat < 109) && (num_mbat != 0) ) return false;
  if( s_shift > b_shift ) return false;
  if( b_shift <= 6 ) return false;
  if( b_shift >=31 ) return false;
  
  return true;
}

void Header::load( const unsigned char* buffer ) {
  b_shift      = readU16( buffer + 0x1e ); // [1EH,02] size of sectors in power-of-two; typically 9 indicating 512-byte sectors and 12 for 4096
  s_shift      = readU16( buffer + 0x20 ); // [20H,02] size of mini-sectors in power-of-two; typically 6 indicating 64-byte mini-sectors
  num_bat      = readU32( buffer + 0x2c ); // [2CH,04] number of SECTs in the FAT chain
  dirent_start = readU32( buffer + 0x30 ); // [30H,04] first SECT in the directory chain
  threshold    = readU32( buffer + 0x38 ); // [38H,04] maximum size for a mini stream; typically 4096 bytes
  sbat_start   = readU32( buffer + 0x3c ); // [3CH,04] first SECT in the MiniFAT chain
  num_sbat     = readU32( buffer + 0x40 ); // [40H,04] number of SECTs in the MiniFAT chain
  mbat_start   = readU32( buffer + 0x44 ); // [44H,04] first SECT in the DIFAT chain
  num_mbat     = readU32( buffer + 0x48 ); // [48H,04] number of SECTs in the DIFAT chain
  
  for( unsigned i = 0; i < 8; i++ )
    id[i] = buffer[i]; 

  // [4CH,436] the SECTs of first 109 FAT sectors
  for( unsigned i=0; i<109; i++ )
    bb_blocks[i] = readU32( buffer + 0x4C+i*4 );
  dirty = false;
}

void Header::save( unsigned char* buffer )
{
  memset( buffer, 0, 0x4c );
  memcpy( buffer, pole_magic, 8 );        // ole signature
  writeU32( buffer + 8, 0 );              // unknown 
  writeU32( buffer + 12, 0 );             // unknown
  writeU32( buffer + 16, 0 );             // unknown
  writeU16( buffer + 24, 0x003e );        // revision ?
  writeU16( buffer + 26, 3 );             // version ?
  writeU16( buffer + 28, 0xfffe );        // unknown
  writeU16( buffer + 0x1e, b_shift );
  writeU16( buffer + 0x20, s_shift );
  writeU32( buffer + 0x2c, num_bat );
  writeU32( buffer + 0x30, dirent_start );
  writeU32( buffer + 0x38, threshold );
  writeU32( buffer + 0x3c, sbat_start );
  writeU32( buffer + 0x40, num_sbat );
  writeU32( buffer + 0x44, mbat_start );
  writeU32( buffer + 0x48, num_mbat );
  
  for( unsigned i=0; i<109; i++ )
    writeU32( buffer + 0x4C+i*4, bb_blocks[i] );
  dirty = false;
}

void Header::debug()
{
  std::cout << std::endl;
  std::cout << "b_shift " << b_shift << std::endl;
  std::cout << "s_shift " << s_shift << std::endl;
  std::cout << "num_bat " << num_bat << std::endl;
  std::cout << "dirent_start " << dirent_start << std::endl;
  std::cout << "threshold " << threshold << std::endl;
  std::cout << "sbat_start " << sbat_start << std::endl;
  std::cout << "num_sbat " << num_sbat << std::endl;
  std::cout << "mbat_start " << mbat_start << std::endl;
  std::cout << "num_mbat " << num_mbat << std::endl;
  
  unsigned s = (num_bat<=109) ? num_bat : 109;
  std::cout << "bat blocks: ";
  for( unsigned i = 0; i < s; i++ )
    std::cout << bb_blocks[i] << " ";
  std::cout << std::endl;
}
 
// =========== AllocTable ==========

const unsigned AllocTable::Avail = 0xffffffff;
const unsigned AllocTable::Eof = 0xfffffffe;
const unsigned AllocTable::Bat = 0xfffffffd;
const unsigned AllocTable::MetaBat = 0xfffffffc;

AllocTable::AllocTable()
{
  blockSize = 4096;
  // initial size
  resize( 128 );
}

unsigned int AllocTable::count()
{
  return (unsigned int) data.size();
}

unsigned int AllocTable::unusedCount()
{
    unsigned int maxIdx = count();
    unsigned int nFound = 0;
    for (unsigned int idx = 0; idx < maxIdx; idx++)
    {
        if( data[idx] == Avail )
            nFound++;
    }
    return nFound;
}

void AllocTable::resize( unsigned int newsize )
{
  unsigned int oldsize = (unsigned int) data.size();
  data.resize( newsize );
  if( newsize > oldsize )
    for( unsigned i = oldsize; i<newsize; i++ )
      data[i] = Avail;
}

// make sure there're still free blocks
void AllocTable::preserve( unsigned int n )
{
  std::vector<unsigned int> pre;
  for( unsigned i=0; i < n; i++ )
    pre.push_back( unused() );
}

unsigned int AllocTable::operator[]( unsigned int index )
{
  unsigned int result;
  result = data[index];
  return result;
}

void AllocTable::set( unsigned int index, unsigned int value )
{
  if( index >= count() ) resize( index + 1);
  data[ index ] = value;
  if (value == Avail)
      bMaybeFragmented = true;
}

void AllocTable::setChain( std::vector<unsigned int> chain )
{
  if( chain.size() )
  {
    for( unsigned i=0; i<chain.size()-1; i++ )
      set( chain[i], chain[i+1] );
    set( chain[ chain.size()-1 ], AllocTable::Eof );
  }
}

// follow 
std::vector<unsigned int> AllocTable::follow( unsigned int start )
{
  std::vector<unsigned int> chain;

  if( start >= count() ) return chain; 

  unsigned int p = start;
  while( p < count() )
  {
    if( p == (unsigned int)Eof ) break;
    if( p == (unsigned int)Bat ) break;
    if( p == (unsigned int)MetaBat ) break;
    if( p >= count() ) break;
    chain.push_back( p );
    if( data[p] >= count() ) break;
    p = data[ p ];
  }

  return chain;
}

unsigned AllocTable::unused()
{
  // find first available block
  unsigned int maxIdx = (unsigned int) data.size();
  if (bMaybeFragmented)
  {
      for( unsigned i = 0; i < maxIdx; i++ )
        if( data[i] == Avail )
          return i;
  }
  
  // completely full, so enlarge the table
  unsigned int block = maxIdx;
  resize( maxIdx );
  bMaybeFragmented = false;
  return block;      
}

void AllocTable::load( const unsigned char* buffer, unsigned len )
{
  resize( len / 4 );
  for( unsigned i = 0; i < count(); i++ )
    set( i, readU32( buffer + i*4 ) );
}

// return space required to save this dirtree
unsigned int AllocTable::size()
{
  return count() * 4;
}

void AllocTable::save( unsigned char* buffer )
{
  for( unsigned i = 0; i < count(); i++ )
    writeU32( buffer + i*4, data[i] );
}

bool AllocTable::isDirty()
{
    return (dirtyBlocks.size() > 0);
}

void AllocTable::markAsDirty(unsigned int dataIndex, int bigBlockSize)
{
    unsigned int dbidx = dataIndex / (bigBlockSize / sizeof(unsigned int));
    for (unsigned int idx = 0; idx < dirtyBlocks.size(); idx++)
    {
        if (dirtyBlocks[idx] == dbidx)
            return;
    }
    dirtyBlocks.push_back(dbidx);
}

void AllocTable::flush(std::vector<unsigned int> blocks, StorageIO *const io, int bigBlockSize)
{
    unsigned char *buffer = new unsigned char[bigBlockSize * blocks.size()];
    save(buffer);
    for (unsigned int idx = 0; idx < blocks.size(); idx++)
    {
        bool bDirty = false;
        for (unsigned int idx2 = 0; idx2 < dirtyBlocks.size(); idx2++)
        {
            if (dirtyBlocks[idx2] == idx)
            {
                bDirty = true;
                break;
            }
        }
        if (bDirty)
            io->saveBigBlock(blocks[idx], 0, &buffer[bigBlockSize*idx], bigBlockSize);
    }
    dirtyBlocks.clear();
    delete[] buffer;
}

void AllocTable::debug()
{
  std::cout << "block size " << data.size() << std::endl;
  for( unsigned i=0; i< data.size(); i++ )
  {
     if( data[i] == Avail ) continue;
     std::cout << i << ": ";
     if( data[i] == Eof ) std::cout << "[eof]";
     else if( data[i] == Bat ) std::cout << "[bat]";
     else if( data[i] == MetaBat ) std::cout << "[metabat]";
     else std::cout << data[i];
     std::cout << std::endl;
  }
}

// =========== DirEntry ==========
// "A node with a shorter name is less than a node with a inter name"
// "For nodes with the same length names, compare the two names." 
// --Windows Compound Binary File Format Specification, Section 2.5
int DirEntry::compare(const DirEntry& de)
{
    return compare(de.name);
}

int DirEntry::compare(const std::string& name2)
{
    if (name.length() < name2.length())
        return -1;
    else if (name.length() > name2.length())
        return 1;
    else
        return name.compare(name2);
}


// =========== DirTree ==========

const unsigned DirTree::End = 0xffffffff;

DirTree::DirTree(int bigBlockSize)
{
  clear(bigBlockSize);
}

void DirTree::clear(int bigBlockSize)
{
  // leave only root entry
  entries.resize( 1 );
  entries[0].valid = true;
  entries[0].name = "Root Entry";
  entries[0].dir = true;
  entries[0].size = 0;
  entries[0].start = End;
  entries[0].prev = End;
  entries[0].next = End;
  entries[0].child = End;
  markAsDirty(0, bigBlockSize);
}

inline unsigned int DirTree::entryCount()
{
  return (unsigned int) entries.size();
}

unsigned int DirTree::unusedEntryCount()
{
    unsigned int nFound = 0;
    for (unsigned idx = 0; idx < entryCount(); idx++)
    {
        if (!entries[idx].valid)
            nFound++;
    }
    return nFound;
}

DirEntry* DirTree::entry( unsigned index )
{
  if( index >= entryCount() ) return (DirEntry*) 0;
  return &entries[ index ];
}

int DirTree::indexOf( DirEntry* e )
{
  for( unsigned i = 0; i < entryCount(); i++ )
    if( entry( i ) == e ) return i;
    
  return -1;
}

int DirTree::parent( unsigned index )
{
  // brute-force, basically we iterate for each entries, find its children
  // and check if one of the children is 'index'
  for( unsigned j=0; j<entryCount(); j++ )
  {
    std::vector<unsigned> chi = children( j );
    for( unsigned i=0; i<chi.size();i++ )
      if( chi[i] == index )
        return j;
  }
        
  return -1;
}

std::string DirTree::fullName( unsigned index )
{
  // don't use root name ("Root Entry"), just give "/"
  if( index == 0 ) return "/";

  std::string result = entry( index )->name;
  result.insert( 0,  "/" );
  int p = parent( index );
  DirEntry * _entry = 0;
  while( p > 0 )
  {
    _entry = entry( p );
    if (_entry->dir && _entry->valid)
    {
      result.insert( 0,  _entry->name);
      result.insert( 0,  "/" );
    }
    --p;
    index = p;
    if( index <= 0 ) break;
  }
  return result;
}

// given a fullname (e.g "/ObjectPool/_1020961869"), find the entry
// if not found and create is false, return 0
// if create is true, a new entry is returned
DirEntry* DirTree::entry( const std::string& name, bool create, int bigBlockSize, StorageIO *const io, int streamSize)
{
   if( !name.length() ) return (DirEntry*)0;
 
   // quick check for "/" (that's root)
   if( name == "/" ) return entry( 0 );
   
   // split the names, e.g  "/ObjectPool/_1020961869" will become:
   // "ObjectPool" and "_1020961869" 
   std::list<std::string> names;
   std::string::size_type start = 0, end = 0;
   if( name[0] == '/' ) start++;
   int levelsLeft = 0;
   while( start < name.length() )
   {
     end = name.find_first_of( '/', start );
     if( end == std::string::npos ) end = name.length();
     names.push_back( name.substr( start, end-start ) );
     levelsLeft++;
     start = end+1;
   }
  
   // start from root 
   int index = 0 ;

   // trace one by one   
   std::list<std::string>::iterator it; 

   for( it = names.begin(); it != names.end(); ++it )
   {
     // find among the children of index
     levelsLeft--;
     unsigned child = 0;

     
     /*
     // dima: this block is really inefficient
     std::vector<unsigned> chi = children( index );
     for( unsigned i = 0; i < chi.size(); i++ )
     {
       DirEntry* ce = entry( chi[i] );
       if( ce ) 
       if( ce->valid && ( ce->name.length()>1 ) )
       if( ce->name == *it ) {
             child = chi[i];
             break;
       }
     }
     */
     // dima: performance optimisation of the previous
     unsigned closest = End;
     child = find_child( index, *it, closest );
     
     // traverse to the child
     if( child > 0 ) index = child;
     else
     {
       // not found among children
       if( !create || !io->writeable) return (DirEntry*)0;
       
       // create a new entry
       unsigned parent = index;
       index = unused();
       DirEntry* e = entry( index );
       e->valid = true;
       e->name = *it;
       e->dir = (levelsLeft > 0);
       if (!e->dir)
           e->size = streamSize;
       else
           e->size = 0;
       e->start = AllocTable::Eof;
       e->child = End;
       if (closest == End)
       {
           e->prev = End;
           e->next = entry(parent)->child;
           entry(parent)->child = index;
           markAsDirty(parent, bigBlockSize);
       }
       else
       {
           DirEntry* closeE = entry( closest );
           if (closeE->compare(*e) < 0)
           {
               e->prev = closeE->next;
               e->next = End;
               closeE->next = index;
           }
           else
           {
               e->next = closeE->prev;
               e->prev = End;
               closeE->prev = index;
           }
           markAsDirty(closest, bigBlockSize);
       }
       markAsDirty(index, bigBlockSize);
       unsigned int bbidx = index / (bigBlockSize / 128);
       std::vector <unsigned int> blocks = io->bbat->follow(io->header->dirent_start);
       while (blocks.size() <= bbidx)
       {
           unsigned int nblock = io->bbat->unused();
           if (blocks.size() > 0)
           {
               io->bbat->set(blocks[(unsigned int)blocks.size()-1], nblock);
               io->bbat->markAsDirty(blocks[blocks.size()-1], bigBlockSize);
           }
           io->bbat->set(nblock, AllocTable::Eof);
           io->bbat->markAsDirty(nblock, bigBlockSize);
           blocks.push_back(nblock);
           unsigned int bbidx = nblock / (io->bbat->blockSize / sizeof(unsigned int));
           while (bbidx >= io->header->num_bat)
               io->addbbatBlock();
       }
     }
   }

   return entry( index );
}

// helper function: recursively find siblings of index
void dirtree_find_siblings( DirTree* dirtree, std::vector<unsigned>& result, 
  unsigned index )
{
    DirEntry* e = dirtree->entry( index );
    if (e->prev != DirTree::End)
        dirtree_find_siblings(dirtree, result, e->prev);
    result.push_back(index);
    if (e->next != DirTree::End)
        dirtree_find_siblings(dirtree, result, e->next);
}

std::vector<unsigned> DirTree::children( unsigned index )
{
  std::vector<unsigned> result;
  
  DirEntry* e = entry( index );
  if( e ) if( e->valid && e->child < entryCount() )
    dirtree_find_siblings( this, result, e->child );
    
  return result;
}

unsigned dirtree_find_sibling( DirTree* dirtree, unsigned index, const std::string& name, unsigned &closest ) {

    unsigned count = dirtree->entryCount();
    DirEntry* e = dirtree->entry( index );
    if (!e || !e->valid) return 0;
    int cval = e->compare(name);
    if (cval == 0)
        return index;
    if (cval > 0)
    {
        if (e->prev > 0 && e->prev < count)
            return dirtree_find_sibling( dirtree, e->prev, name, closest );
    }
    else
    {
        if (e->next > 0 && e->next < count)
            return dirtree_find_sibling( dirtree, e->next, name, closest );
    }
    closest = index;
    return 0;
}

unsigned DirTree::find_child( unsigned index, const std::string& name, unsigned& closest ) {

  unsigned count = entryCount();
  DirEntry* p = entry( index );
  if (p && p->valid && p->child < count )
    return dirtree_find_sibling( this, p->child, name, closest );
  
  return 0;
}

void DirTree::load( unsigned char* buffer, unsigned size )
{
  entries.clear();
  
  for( unsigned i = 0; i < size/128; i++ )
  {
    unsigned p = i * 128;
    
    // would be < 32 if first char in the name isn't printable
    unsigned prefix = 32;
    
    // parse name of this entry, which stored as Unicode 16-bit
    std::string name;
    int name_len = readU16( buffer + 0x40+p );
    if( name_len > 64 ) name_len = 64;
    for( int j=0; ( buffer[j+p]) && (j<name_len); j+= 2 )
      name.append( 1, buffer[j+p] );
      
    // first char isn't printable ? remove it...
    if( buffer[p] < 32 )
    { 
      prefix = buffer[0]; 
      name.erase( 0,1 ); 
    }
    
    // 2 = file (aka stream), 1 = directory (aka storage), 5 = root
    unsigned type = buffer[ 0x42 + p];
    
    DirEntry e;
    e.valid = ( type != 0 );
    e.name = name;
    e.start = readU32( buffer + 0x74+p );
    e.size = readU32( buffer + 0x78+p );
    e.prev = readU32( buffer + 0x44+p );
    e.next = readU32( buffer + 0x48+p );
    e.child = readU32( buffer + 0x4C+p );
    e.dir = ( type!=2 );
    
    // sanity checks
    if( (type != 2) && (type != 1 ) && (type != 5 ) ) e.valid = false;
    if( name_len < 1 ) e.valid = false;
    
    entries.push_back( e );
  }  
}

// return space required to save this dirtree
unsigned DirTree::size()
{
  return entryCount() * 128;
}

void DirTree::save( unsigned char* buffer )
{
  memset( buffer, 0, size() );
  
  // root is fixed as "Root Entry"
  DirEntry* root = entry( 0 );
  std::string name = "Root Entry";
  for( unsigned j = 0; j < name.length(); j++ )
    buffer[ j*2 ] = name[j];
  writeU16( buffer + 0x40, (int) name.length()*2 + 2 );
  writeU32( buffer + 0x74, 0xffffffff );
  writeU32( buffer + 0x78, 0 );
  writeU32( buffer + 0x44, 0xffffffff );
  writeU32( buffer + 0x48, 0xffffffff );
  writeU32( buffer + 0x4c, root->child );
  buffer[ 0x42 ] = 5;
  //buffer[ 0x43 ] = 1; 

  for( unsigned i = 1; i < entryCount(); i++ )
  {
    DirEntry* e = entry( i );
    if( !e ) continue;
    if( e->dir )
    {
      e->start = 0xffffffff;
      e->size = 0;
    }
    
    // max length for name is 32 chars
    std::string name = e->name;
    if( name.length() > 32 )
      name.erase( 32, name.length() );
      
    // write name as Unicode 16-bit
    for( unsigned j = 0; j < name.length(); j++ )
      buffer[ i*128 + j*2 ] = name[j];

    writeU16( buffer + i*128 + 0x40, (int) name.length()*2 + 2 );
    writeU32( buffer + i*128 + 0x74, e->start );
    writeU32( buffer + i*128 + 0x78, e->size );
    writeU32( buffer + i*128 + 0x44, e->prev );
    writeU32( buffer + i*128 + 0x48, e->next );
    writeU32( buffer + i*128 + 0x4c, e->child );
    if (!e->valid)
        buffer[ i*128 + 0x42 ] = 0; //STGTY_INVALID
    else
        buffer[ i*128 + 0x42 ] = e->dir ? 1 : 2; //STGTY_STREAM or STGTY_STORAGE
    buffer[ i*128 + 0x43 ] = 1; // always black
  }  
}

bool DirTree::isDirty()
{
    return (dirtyBlocks.size() > 0);
}


void DirTree::markAsDirty(unsigned int dataIndex, int bigBlockSize)
{
    unsigned int dbidx = dataIndex / (bigBlockSize / 128);
    for (unsigned int idx = 0; idx < dirtyBlocks.size(); idx++)
    {
        if (dirtyBlocks[idx] == dbidx)
            return;
    }
    dirtyBlocks.push_back(dbidx);
}

void DirTree::flush(std::vector<unsigned int> blocks, StorageIO *const io, int bigBlockSize, unsigned int sb_start, unsigned int sb_size)
{
    unsigned int bufLen = size();
    unsigned char *buffer = new unsigned char[bufLen];
    save(buffer);
    writeU32( buffer + 0x74, sb_start );
    writeU32( buffer + 0x78, sb_size );
    for (unsigned int idx = 0; idx < blocks.size(); idx++)
    {
        bool bDirty = false;
        for (unsigned int idx2 = 0; idx2 < dirtyBlocks.size(); idx2++)
        {
            if (dirtyBlocks[idx2] == idx)
            {
                bDirty = true;
                break;
            }
        }
        unsigned int bytesToWrite = bigBlockSize;
        unsigned int pos = bigBlockSize*idx;
        if ((bufLen - pos) < bytesToWrite)
            bytesToWrite = bufLen - pos;
        if (bDirty)
            io->saveBigBlock(blocks[idx], 0, &buffer[pos], bytesToWrite);
    }
    dirtyBlocks.clear();
    delete[] buffer;
}

unsigned DirTree::unused()
{
    for (unsigned idx = 0; idx < entryCount(); idx++)
    {
        if (!entries[idx].valid)
            return idx;
    }
    entries.push_back(DirEntry());
    return entryCount()-1;
}

// Utility function to get the index of the parent dirEntry, given that we already have a full name it is relatively fast.
// Then look for a sibling dirEntry that points to inIdx. In some circumstances, the dirEntry at inIdx will be the direct child
// of the parent, in which case sibIdx will be returned as 0. A failure is indicated if both parentIdx and sibIdx are returned as 0.

void DirTree::findParentAndSib(unsigned inIdx, const std::string& inFullName, unsigned &parentIdx, unsigned &sibIdx)
{
    sibIdx = 0;
    parentIdx = 0;
    if (inIdx == 0 || inIdx >= entryCount() || inFullName == "/" || inFullName == "")
        return;
    std::string localName = inFullName;
    if (localName[0] != '/')
        localName = '/' + localName;
    std::string parentName = localName;
    if (parentName[parentName.size()-1] == '/')
        parentName = parentName.substr(0, parentName.size()-1);
    std::string::size_type lastSlash;
    lastSlash = parentName.find_last_of('/');
    if (lastSlash == std::string::npos)
        return;
    if (lastSlash == 0)
        lastSlash = 1; //leave root
    parentName = parentName.substr(0, lastSlash);
    DirEntry *parent = entry(parentName);
    parentIdx = indexOf(parent);
    if (parent->child == inIdx)
        return; //successful return, no sibling points to inIdx
    sibIdx = findSib(inIdx, parent->child);
}

// Utility function to get the index of the sibling dirEntry which points to inIdx. It is the responsibility of the original caller
// to start with the root sibling - i.e., sibIdx should be pointed to by the parent node's child.

unsigned DirTree::findSib(unsigned inIdx, unsigned sibIdx)
{
    DirEntry *sib = entry(sibIdx);
    if (!sib || !sib->valid)
        return 0;
    if (sib->next == inIdx || sib->prev == inIdx)
        return sibIdx;
    DirEntry *targetSib = entry(inIdx);
    int cval = sib->compare(*targetSib);
    if (cval > 0)
        return findSib(inIdx, sib->prev);
    else
        return findSib(inIdx, sib->next);
}

void DirTree::deleteEntry(DirEntry *dirToDel, const std::string& inFullName, int bigBlockSize)
{
    unsigned parentIdx;
    unsigned sibIdx;
    unsigned inIdx = indexOf(dirToDel);
    unsigned nEntries = entryCount();
    findParentAndSib(inIdx, inFullName, parentIdx, sibIdx);
    unsigned replIdx;
    if (!dirToDel->next || dirToDel->next > nEntries)
        replIdx = dirToDel->prev;
    else
    {
        DirEntry *sibNext = entry(dirToDel->next);
        if (!sibNext->prev || sibNext->prev > nEntries)
        {
            replIdx = dirToDel->next;
            sibNext->prev = dirToDel->prev;
            markAsDirty(replIdx, bigBlockSize);
        }
        else
        {
            DirEntry *smlSib = sibNext;
            int smlIdx = dirToDel->next;
            DirEntry *smlrSib;
            int smlrIdx = -1;
            for ( ; ; )
            {
                smlrIdx = smlSib->prev;
                smlrSib = entry(smlrIdx);
                if (!smlrSib->prev || smlrSib->prev > nEntries)
                    break;
                smlSib = smlrSib;
                smlIdx = smlrIdx;
            }
            replIdx = smlSib->prev;
            smlSib->prev = smlrSib->next;
            smlrSib->prev = dirToDel->prev;
            smlrSib->next = dirToDel->next;
            markAsDirty(smlIdx, bigBlockSize);
            markAsDirty(smlrIdx, bigBlockSize);
        }
    }
    if (sibIdx)
    {
        DirEntry *sib = entry(sibIdx);
        if (sib->next == inIdx)
            sib->next = replIdx;
        else
            sib->prev = replIdx;
        markAsDirty(sibIdx, bigBlockSize);
    }
    else
    {
        DirEntry *parNode = entry(parentIdx);
        parNode->child = replIdx;
        markAsDirty(parentIdx, bigBlockSize);
    }
    dirToDel->valid = false; //indicating that this entry is not in use
    markAsDirty(inIdx, bigBlockSize);
}


void DirTree::debug()
{
  for( unsigned i = 0; i < entryCount(); i++ )
  {
    DirEntry* e = entry( i );
    if( !e ) continue;
    std::cout << i << ": ";
    if( !e->valid ) std::cout << "INVALID ";
    std::cout << e->name << " ";
    if( e->dir ) std::cout << "(Dir) ";
    else std::cout << "(File) ";
    std::cout << e->size << " ";
    std::cout << "s:" << e->start << " ";
    std::cout << "(";
    if( e->child == End ) std::cout << "-"; else std::cout << e->child;
    std::cout << " ";
    if( e->prev == End ) std::cout << "-"; else std::cout << e->prev;
    std::cout << ":";
    if( e->next == End ) std::cout << "-"; else std::cout << e->next;
    std::cout << ")";    
    std::cout << std::endl;
  }
}

// =========== StorageIO ==========

StorageIO::StorageIO( Storage* st, const char* fname )
{
  storage = st;
  filename = fname;
  result = Storage::Ok;
  opened = false;
  
  header = new Header();
  bbat = new AllocTable();
  sbat = new AllocTable();
  
  filesize = 0;
  bbat->blockSize = 1 << header->b_shift;
  sbat->blockSize = 1 << header->s_shift;
  dirtree = new DirTree(bbat->blockSize);
  writeable = false;
}

StorageIO::~StorageIO()
{
  if( opened ) close();
  delete sbat;
  delete bbat;
  delete dirtree;
  delete header;
}

bool StorageIO::open(bool bWriteAccess, bool bCreate)
{
  // already opened ? close first
  if (opened)
      close();
  if (bCreate)
  {
      create();
      init();
      writeable = true;
  }
  else
  {
      writeable = bWriteAccess;
      load(bWriteAccess);
  }
  
  return result == Storage::Ok;
}

void StorageIO::load(bool bWriteAccess)
{
  unsigned char* buffer = 0;
  unsigned int buflen = 0;
  std::vector<unsigned int> blocks;
  
  // open the file, check for error
  result = Storage::OpenFailed;
  if (bWriteAccess)
      file.open( filename.c_str(), std::ios::binary | std::ios::in | std::ios::out );
  else
      file.open( filename.c_str(), std::ios::binary | std::ios::in );
  if( !file.good() ) return;
  
  // find size of input file
  file.seekg( (unsigned int) 0, std::ios::end );
  filesize = (unsigned long)file.tellg();

  // load header
  buffer = new unsigned char[512];
  file.seekg( 0 ); 
  file.read( (char*)buffer, 512 );
  fileCheck(file);
  header->load( buffer );
  delete[] buffer;

  // check OLE magic id
  result = Storage::NotOLE;
  for( unsigned i=0; i<8; i++ )
    if( header->id[i] != pole_magic[i] )
      return;
  
  // sanity checks
  result = Storage::BadOLE;
  if( !header->valid() ) return;
  if( header->threshold != 4096 ) return;

  // important block size
  bbat->blockSize = 1 << header->b_shift;
  sbat->blockSize = 1 << header->s_shift;
  
  blocks = getbbatBlocks(true);
  
  // load big bat
  buflen = (unsigned int) blocks.size()*bbat->blockSize;
  if( buflen > 0 )
  {
    buffer = new unsigned char[ buflen ];  
    loadBigBlocks( blocks, buffer, buflen );
    bbat->load( buffer, buflen );
    delete[] buffer;
  }  

  // load small bat
  blocks.clear();
  blocks = bbat->follow( header->sbat_start );
  buflen = (unsigned int) blocks.size()*bbat->blockSize;
  if( buflen > 0 )
  {
    buffer = new unsigned char[ buflen ];  
    loadBigBlocks( blocks, buffer, buflen );
    sbat->load( buffer, buflen );
    delete[] buffer;
  }  
  
  // load directory tree
  blocks.clear();
  blocks = bbat->follow( header->dirent_start );
  buflen = (unsigned int) blocks.size()*bbat->blockSize;
  buffer = new unsigned char[ buflen ];  
  loadBigBlocks( blocks, buffer, buflen );
  dirtree->load( buffer, buflen );
  unsigned sb_start = readU32( buffer + 0x74 );
  delete[] buffer;
  
  // fetch block chain as data for small-files
  sb_blocks = bbat->follow( sb_start ); // small files
  
  // for troubleshooting, just enable this block
#if 0
  header->debug();
  sbat->debug();
  bbat->debug();
  dirtree->debug();
#endif
  
  // so far so good
  result = Storage::Ok;
  opened = true;
}

void StorageIO::create()
{
  // std::cout << "Creating " << filename << std::endl; 
  
  file.open( filename.c_str(), std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
  if( !file.good() )
  {
    std::cerr << "Can't create " << filename << std::endl;
    result = Storage::OpenFailed;
    return;
  }
  
  // so far so good
  opened = true;
  result = Storage::Ok;
}

void StorageIO::init()
{
    // Initialize parts of the header, directory entries, and big and small allocation tables
    header->bb_blocks[0] = 0;
    header->dirent_start = 1;
    header->sbat_start = 2;
    header->num_bat = 1;
    header->num_sbat = 1;
    header->dirty = true;
    bbat->set(0, AllocTable::Eof);
    bbat->markAsDirty(0, bbat->blockSize);
    bbat->set(1, AllocTable::Eof);
    bbat->markAsDirty(1, bbat->blockSize);
    bbat->set(2, AllocTable::Eof);
    bbat->markAsDirty(2, bbat->blockSize);
    bbat->set(3, AllocTable::Eof);
    bbat->markAsDirty(3, bbat->blockSize);
    sb_blocks = bbat->follow( 3 );
    mbatDirty = false;  
}

void StorageIO::flush()
{
    if (header->dirty)
    {
        unsigned char *buffer = new unsigned char[512];
        header->save( buffer );
        file.seekp( 0 ); 
        file.write( (char*)buffer, 512 );
        fileCheck(file);
        delete[] buffer;
    }
    if (bbat->isDirty())
        flushbbat();
    if (sbat->isDirty())
        flushsbat();
    if (dirtree->isDirty())
    {
        std::vector<unsigned int> blocks;
        blocks = bbat->follow(header->dirent_start);
        unsigned int sb_start = 0xffffffff;
        if (sb_blocks.size() > 0)
            sb_start = sb_blocks[0];
        dirtree->flush(blocks, this, bbat->blockSize, sb_start, (unsigned int) sb_blocks.size()*bbat->blockSize);
    }
    if (mbatDirty && mbat_blocks.size() > 0)
    {
        unsigned int nBytes = bbat->blockSize * (unsigned int) mbat_blocks.size();
        unsigned char *buffer = new unsigned char[nBytes];
        unsigned int sIdx = 0;
        unsigned int dcount = 0;
        unsigned blockCapacity = bbat->blockSize / sizeof(unsigned int) - 1;
        unsigned blockIdx = 0;
        for (unsigned mdIdx = 0; mdIdx < mbat_data.size(); mdIdx++)
        {
            writeU32(buffer + sIdx, mbat_data[mdIdx]);
            sIdx += 4;
            dcount++;
            if (dcount == blockCapacity)
            {
                blockIdx++;
                if (blockIdx == mbat_blocks.size())
                    writeU32(buffer + sIdx, AllocTable::Eof);
                else
                    writeU32(buffer + sIdx, mbat_blocks[blockIdx]);
                sIdx += 4;
                dcount = 0;
            }
        }
        saveBigBlocks(mbat_blocks, 0, buffer, nBytes);
        delete[] buffer;
        mbatDirty = false;
    }
    file.flush();
    fileCheck(file);

  /* Note on Microsoft implementation:
     - directory entries are stored in the last block(s)
     - BATs are as second to the last
     - Meta BATs are third to the last  
  */
}

void StorageIO::close()
{
  if( !opened ) return;
  
  file.close(); 
  opened = false;
  
  std::list<Stream*>::iterator it;
  for( it = streams.begin(); it != streams.end(); ++it )
    delete *it;
}


StreamIO* StorageIO::streamIO( const std::string& name, bool bCreate, int streamSize )
{
  // sanity check
  if( !name.length() ) return (StreamIO*)0;

  // search in the entries
  DirEntry* entry = dirtree->entry( name, bCreate, bbat->blockSize, this, streamSize );
  //if( entry) std::cout << "FOUND\n";
  if( !entry ) return (StreamIO*)0;
  //if( !entry->dir ) std::cout << "  NOT DIR\n";
  if( entry->dir ) return (StreamIO*)0;

  StreamIO* result = new StreamIO( this, entry );
  result->fullName = name;
  
  return result;
}

bool StorageIO::deleteByName(const std::string& fullName)
{
    if (!fullName.length())
        return false;
    if (!writeable)
        return false;
    DirEntry* entry = dirtree->entry(fullName);
    if (!entry)
        return false;
    bool retVal;
    if (entry->dir)
        retVal = deleteNode(entry, fullName);
    else
        retVal = deleteLeaf(entry, fullName);
    if (retVal)
        flush();
    return retVal;
}

bool StorageIO::deleteNode(DirEntry *entry, const std::string& fullName)
{
    std::string lclName = fullName;
    if (lclName[lclName.size()-1] != '/')
        lclName += '/';
    bool retVal = true;
    while (entry->child && entry->child < dirtree->entryCount())
    {
        DirEntry* childEnt = dirtree->entry(entry->child);
        std::string childFullName = lclName + childEnt->name;
        if (childEnt->dir)
            retVal = deleteNode(childEnt, childFullName);
        else
            retVal = deleteLeaf(childEnt, childFullName);
        if (!retVal)
            return false;
    }
    dirtree->deleteEntry(entry, fullName, bbat->blockSize);
    return retVal;
}

bool StorageIO::deleteLeaf(DirEntry *entry, const std::string& fullName)
{
    std::vector<unsigned int> blocks;
    if (entry->size >= header->threshold)
    {
        blocks = bbat->follow(entry->start);
        for (unsigned idx = 0; idx < blocks.size(); idx++)
        {
            bbat->set(blocks[idx], AllocTable::Avail);
            bbat->markAsDirty(idx, bbat->blockSize);
        }
    }
    else
    {
        blocks = sbat->follow(entry->start);
        for (unsigned idx = 0; idx < blocks.size(); idx++)
        {
            sbat->set(blocks[idx], AllocTable::Avail);
            sbat->markAsDirty(idx, bbat->blockSize);
        }
    }
    dirtree->deleteEntry(entry, fullName, bbat->blockSize);
    return true;
}

unsigned int StorageIO::loadBigBlocks( std::vector<unsigned int> blocks,
  unsigned char* data, unsigned int maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( maxlen == 0 ) return 0;

  // read block one by one, seems fast enough
  unsigned int bytes = 0;
  for( unsigned int i=0; (i < blocks.size() ) & ( bytes<maxlen ); i++ )
  {
    unsigned int block = blocks[i];
    unsigned int pos =  bbat->blockSize * ( block+1 );
    unsigned long p = (bbat->blockSize < maxlen-bytes) ? bbat->blockSize : maxlen-bytes;
    if( pos + p > filesize )
        p = filesize - pos;
    file.seekg( pos );
    file.read( (char*)data + bytes, p );
    fileCheck(file);
    // should use gcount to see how many bytes were really returned - eof check...
    bytes += p;
  }

  return bytes;
}

unsigned int StorageIO::loadBigBlock( unsigned int block,
  unsigned char* data, unsigned int maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  
  // wraps call for loadBigBlocks
  std::vector<unsigned int> blocks;
  blocks.resize( 1 );
  blocks[ 0 ] = block;
  
  return loadBigBlocks( blocks, data, maxlen );
}

unsigned int StorageIO::saveBigBlocks( std::vector<unsigned int> blocks, unsigned int offset, unsigned char* data, unsigned int len )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( len == 0 ) return 0;

  // write block one by one, seems fast enough
  unsigned int bytes = 0;
  for( unsigned int i=0; (i < blocks.size() ) & ( bytes<len ); i++ )
  {
    unsigned int block = blocks[i];
    unsigned int pos =  (bbat->blockSize * ( block+1 ) ) + offset;
    unsigned int maxWrite = bbat->blockSize - offset;
    unsigned int tobeWritten = len - bytes;
    if (tobeWritten > maxWrite)
        tobeWritten = maxWrite;
    file.seekp( pos );
    file.write( (char*)data + bytes, tobeWritten );
    fileCheck(file);

    bytes += tobeWritten;
    offset = 0;
    if (filesize < pos + tobeWritten)
        filesize = pos + tobeWritten;
  }

  return bytes;

}

unsigned int StorageIO::saveBigBlock( unsigned int block, unsigned int offset, unsigned char* data, unsigned int len )
{
    if ( !data ) return 0;
    fileCheck(file);
    if ( !file.good() ) return 0;
    //wrap call for saveBigBlocks
    std::vector<unsigned int> blocks;
    blocks.resize( 1 );
    blocks[ 0 ] = block;
    return saveBigBlocks(blocks, offset, data, len );
}

// return number of bytes which has been read
unsigned int StorageIO::loadSmallBlocks( std::vector<unsigned int> blocks,
  unsigned char* data, unsigned int maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( maxlen == 0 ) return 0;

  // our own local buffer
  unsigned char* buf = new unsigned char[ bbat->blockSize ];

  // read small block one by one
  unsigned int bytes = 0;
  for( unsigned int i=0; ( i<blocks.size() ) & ( bytes<maxlen ); i++ )
  {
    unsigned int block = blocks[i];

    // find where the small-block exactly is
    unsigned int pos = block * sbat->blockSize;
    unsigned int bbindex = pos / bbat->blockSize;
    if( bbindex >= sb_blocks.size() ) break;

    loadBigBlock( sb_blocks[ bbindex ], buf, bbat->blockSize );

    // copy the data
    unsigned offset = pos % bbat->blockSize;
    unsigned int p = (maxlen-bytes < bbat->blockSize-offset ) ? maxlen-bytes :  bbat->blockSize-offset;
    p = (sbat->blockSize<p ) ? sbat->blockSize : p;
    memcpy( data + bytes, buf + offset, p );
    bytes += p;
  }
  
  delete[] buf;

  return bytes;
}

unsigned int StorageIO::loadSmallBlock( unsigned int block,
  unsigned char* data, unsigned int maxlen )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;

  // wraps call for loadSmallBlocks
  std::vector<unsigned int> blocks;
  blocks.resize( 1 );
  blocks.assign( 1, block );

  return loadSmallBlocks( blocks, data, maxlen );
}


unsigned int StorageIO::saveSmallBlocks( std::vector<unsigned int> blocks, unsigned int offset, 
                                        unsigned char* data, unsigned int len, int startAtBlock )
{
  // sentinel
  if( !data ) return 0;
  fileCheck(file);
  if( !file.good() ) return 0;
  if( blocks.size() < 1 ) return 0;
  if( len == 0 ) return 0;

  // write block one by one, seems fast enough
  unsigned int bytes = 0;
  for( unsigned int i = startAtBlock; (i < blocks.size() ) & ( bytes<len ); i++ )
  {
    unsigned int block = blocks[i];
     // find where the small-block exactly is
    unsigned int pos = block * sbat->blockSize;
    unsigned int bbindex = pos / bbat->blockSize;
    if( bbindex >= sb_blocks.size() ) break;
    unsigned offset2 = pos % bbat->blockSize;
    unsigned int maxWrite = sbat->blockSize - offset;
    unsigned int tobeWritten = len - bytes;
    if (tobeWritten > maxWrite)
        tobeWritten = maxWrite;
    saveBigBlock( sb_blocks[ bbindex ], offset2 + offset, data + bytes, tobeWritten);
    bytes += tobeWritten;
    offset = 0;
    if (filesize < pos + tobeWritten)
        filesize = pos + tobeWritten;
  }
  return bytes;
}

unsigned int StorageIO::saveSmallBlock( unsigned int block, unsigned int offset, unsigned char* data, unsigned int len )
{
    if ( !data ) return 0;
    fileCheck(file);
    if ( !file.good() ) return 0;
    //wrap call for saveSmallBlocks
    std::vector<unsigned int> blocks;
    blocks.resize( 1 );
    blocks[ 0 ] = block;
    return saveSmallBlocks(blocks, offset, data, len );
}

void StorageIO::flushbbat()
{
    std::vector<unsigned int> blocks;
    blocks = getbbatBlocks(false);
    bbat->flush(blocks, this, bbat->blockSize);
}

void StorageIO::flushsbat()
{
    std::vector<unsigned int> blocks;
    blocks = bbat->follow(header->sbat_start);
    sbat->flush(blocks, this, bbat->blockSize);
}

std::vector<unsigned int> StorageIO::getbbatBlocks(bool bLoading)
{
    std::vector<unsigned int> blocks;
    // find blocks allocated to store big bat
    // the first 109 blocks are in header, the rest in meta bat
    blocks.clear();
    blocks.resize( header->num_bat );

    for( unsigned i = 0; i < 109; i++ )
    {
        if( i >= header->num_bat ) 
            break;
        else 
            blocks[i] = header->bb_blocks[i];
    }
    if (bLoading)
    {
        mbat_blocks.clear();
        mbat_data.clear();
        if( (header->num_bat > 109) && (header->num_mbat > 0) ) 
        {
            unsigned char* buffer2 = new unsigned char[ bbat->blockSize ];
            unsigned k = 109;
            unsigned sector;
            unsigned mdidx = 0;
            for( unsigned r = 0; r < header->num_mbat; r++ )
            {
                if(r == 0) // 1st meta bat location is in file header.
                    sector = header->mbat_start;
                else      // next meta bat location is the last current block value.
                {
                    sector = blocks[--k];
                    mdidx--;
                }
                mbat_blocks.push_back(sector);
                mbat_data.resize(mbat_blocks.size()*(bbat->blockSize/4));
                loadBigBlock( sector, buffer2, bbat->blockSize );
                for( unsigned s=0; s < bbat->blockSize; s+=4 )
                {
                    if( k >= header->num_bat )
                        break;
                    else
                    {
                        blocks[k] = readU32( buffer2 + s );
                        mbat_data[mdidx++] = blocks[k];
                        k++;
                    }
                }  
            }
            if (mbat_data.size() != mdidx) mbat_data.resize(mdidx);
            delete[] buffer2;
        }
    }
    else
    {
        unsigned i = 109;
        for (unsigned int idx = 0; idx < mbat_data.size(); idx++)
        {
            blocks[i++] = mbat_data[idx];
            if (i == header->num_bat)
                break;
        }
    }
    return blocks;
}

unsigned int StorageIO::ExtendFile( std::vector<unsigned int> *chain )
{
    unsigned int newblockIdx = bbat->unused();
    bbat->set(newblockIdx, AllocTable::Eof);
    unsigned int bbidx = newblockIdx / (bbat->blockSize / sizeof(unsigned int));
    while (bbidx >= header->num_bat)
        addbbatBlock();
    bbat->markAsDirty(newblockIdx, bbat->blockSize);
    if (chain->size() > 0)
    {
        bbat->set((*chain)[chain->size()-1], newblockIdx);
        bbat->markAsDirty((*chain)[chain->size()-1], bbat->blockSize);
    }
    chain->push_back(newblockIdx);
    return newblockIdx;
}

void StorageIO::addbbatBlock()
{
    unsigned int newblockIdx = bbat->unused();
    bbat->set(newblockIdx, AllocTable::MetaBat);

    if (header->num_bat < 109)
        header->bb_blocks[header->num_bat] = newblockIdx;
    else
    {
        mbatDirty = true;
        mbat_data.push_back(newblockIdx);
        unsigned metaIdx = header->num_bat - 109;
        unsigned idxPerBlock = bbat->blockSize / sizeof(unsigned int) - 1; //reserve room for index to next block
        unsigned idxBlock = metaIdx / idxPerBlock;
        if (idxBlock == mbat_blocks.size())
        {
            unsigned int newmetaIdx = bbat->unused();
            bbat->set(newmetaIdx, AllocTable::MetaBat);
            mbat_blocks.push_back(newmetaIdx);
            if (header->num_mbat == 0)
                header->mbat_start = newmetaIdx;
            header->num_mbat++;
        }
    }
    header->num_bat++;
    header->dirty = true;
}


// =========== StreamIO ==========

StreamIO::StreamIO( StorageIO* s, DirEntry* e)
{
  io = s;
  entryIdx = io->dirtree->indexOf(e);
  eof = false;
  fail = false;
  
  m_pos = 0;

  if( e->size >= io->header->threshold ) 
    blocks = io->bbat->follow( e->start );
  else
    blocks = io->sbat->follow( e->start );

  // prepare cache
  cache_pos = 0;
  cache_size = 0; // indicating an empty cache
  cache_data = new unsigned char[CACHEBUFSIZE];
}

// FIXME tell parent we're gone
StreamIO::~StreamIO()
{
  delete[] cache_data;  
}

void StreamIO::setSize(unsigned int newSize)
{
    bool bThresholdCrossed = false;
    bool bOver = false;

    if(!io->writeable )
        return;
    DirEntry *entry = io->dirtree->entry(entryIdx);
    if (newSize >= io->header->threshold && entry->size < io->header->threshold)
    {
        bThresholdCrossed = true;
        bOver = true;
    }
    else if (newSize < io->header->threshold && entry->size >= io->header->threshold)
    {
        bThresholdCrossed = true;
        bOver = false;
    }
    if (bThresholdCrossed)
    {
        // first, read what is already in the stream, limited by the requested new size. Note
        // that the read can work precisely because we have not yet reset the size.
        unsigned int len = newSize;
        if (len > entry->size)
            len = entry->size;
        unsigned char *buffer = 0;
        unsigned int savePos = tell();
        if (len)
        {
            buffer = new unsigned char[len];
            seek(0);
            read(buffer, len);
        }
        // Now get rid of the existing blocks
        if (bOver)
        {
            for (unsigned int idx = 0; idx < blocks.size(); idx++)
            {
                io->sbat->set(blocks[idx], AllocTable::Avail);
                io->sbat->markAsDirty(idx, io->bbat->blockSize);
            }
        }
        else
        {
            for (unsigned int idx = 0; idx < blocks.size(); idx++)
            {
                io->bbat->set(blocks[idx], AllocTable::Avail);
                io->bbat->markAsDirty(idx, io->bbat->blockSize);
            }
        }
        blocks.clear();
        entry->start = DirTree::End;
        // Now change the size, and write the old data back into the stream, if any
        entry->size = newSize;
        io->dirtree->markAsDirty(io->dirtree->indexOf(entry), io->bbat->blockSize);
        if (len)
        {
            write(0, buffer, len);
            delete buffer;
        }
        if (savePos <= entry->size)
            seek(savePos);
    }
    else if (entry->size != newSize) //simple case - no threshold was crossed, so just change the size
    {
        entry->size = newSize;
        io->dirtree->markAsDirty(io->dirtree->indexOf(entry), io->bbat->blockSize);
    }

}

void StreamIO::seek( unsigned int pos )
{
  m_pos = pos;
}

unsigned int StreamIO::tell()
{
  return m_pos;
}

int StreamIO::getch()
{
  // past end-of-file ?
  DirEntry *entry = io->dirtree->entry(entryIdx);
  if( m_pos >= entry->size ) return -1;

  // need to update cache ?
  if( !cache_size || ( m_pos < cache_pos ) ||
    ( m_pos >= cache_pos + cache_size ) )
      updateCache();

  // something bad if we don't get good cache
  if( !cache_size ) return -1;

  int data = cache_data[m_pos - cache_pos];
  m_pos++;

  return data;
}

unsigned int StreamIO::read( unsigned int pos, unsigned char* data, unsigned int maxlen )
{
  // sanity checks
  if( !data ) return 0;
  if( maxlen == 0 ) return 0;

  unsigned int totalbytes = 0;
  
  DirEntry *entry = io->dirtree->entry(entryIdx);
  if (pos + maxlen > entry->size)
      maxlen = entry->size - pos;
  if ( entry->size < io->header->threshold )
  {
    // small file
    unsigned int index = pos / io->sbat->blockSize;

    if( index >= blocks.size() ) return 0;

    unsigned char* buf = new unsigned char[ io->sbat->blockSize ];
    unsigned int offset = pos % io->sbat->blockSize;
    while( totalbytes < maxlen )
    {
      if( index >= blocks.size() ) break;
      io->loadSmallBlock( blocks[index], buf, io->bbat->blockSize );
      unsigned int count = io->sbat->blockSize - offset;
      if( count > maxlen-totalbytes ) count = maxlen-totalbytes;
      memcpy( data+totalbytes, buf + offset, count );
      totalbytes += count;
      offset = 0;
      index++;
    }
    delete[] buf;

  }
  else
  {
    // big file
    unsigned int index = pos / io->bbat->blockSize;
    
    if( index >= blocks.size() ) return 0;
    
    unsigned char* buf = new unsigned char[ io->bbat->blockSize ];
    unsigned int offset = pos % io->bbat->blockSize;
    while( totalbytes < maxlen )
    {
      if( index >= blocks.size() ) break;
      io->loadBigBlock( blocks[index], buf, io->bbat->blockSize );
      unsigned int count = io->bbat->blockSize - offset;
      if( count > maxlen-totalbytes ) count = maxlen-totalbytes;
      memcpy( data+totalbytes, buf + offset, count );
      totalbytes += count;
      index++;
      offset = 0;
    }
    delete [] buf;

  }

  return totalbytes;
}

unsigned int StreamIO::read( unsigned char* data, unsigned int maxlen )
{
  unsigned int bytes = read( tell(), data, maxlen );
  m_pos += bytes;
  return bytes;
}

unsigned int StreamIO::write( unsigned char* data, unsigned int len )
{
  return write( tell(), data, len );
}

unsigned int StreamIO::write( unsigned int pos, unsigned char* data, unsigned int len )
{
  // sanity checks
  if( !data ) return 0;
  if( len == 0 ) return 0;
  if( !io->writeable ) return 0;

  DirEntry *entry = io->dirtree->entry(entryIdx);
  if (pos + len > entry->size)
      setSize(pos + len); //reset size, possibly changing from small to large blocks
  unsigned int totalbytes = 0;
  if ( entry->size < io->header->threshold )
  {
    // small file
    unsigned int index = (pos + len - 1) / io->sbat->blockSize;
    while (index >= blocks.size())
    {
        unsigned int nblock = io->sbat->unused();
        if (blocks.size() > 0)
        {
            io->sbat->set(blocks[blocks.size()-1], nblock);
            io->sbat->markAsDirty(blocks[blocks.size()-1], io->bbat->blockSize);
        }
        io->sbat->set(nblock, AllocTable::Eof);
        io->sbat->markAsDirty(nblock, io->bbat->blockSize);
        blocks.resize(blocks.size()+1);
        blocks[blocks.size()-1] = nblock;
        unsigned int bbidx = nblock / (io->bbat->blockSize / sizeof(unsigned int));
        while (bbidx >= io->header->num_sbat)
        {
            std::vector<unsigned int> sbat_blocks = io->bbat->follow(io->header->sbat_start);
            io->ExtendFile(&sbat_blocks);
            io->header->num_sbat++;
        }
        unsigned int sidx = nblock * io->sbat->blockSize / io->bbat->blockSize;
        while (sidx >= io->sb_blocks.size())
            io->ExtendFile(&io->sb_blocks);
    }
    unsigned int offset = pos % io->sbat->blockSize;
    index = pos / io->sbat->blockSize;
    //if (index == 0)
        totalbytes = io->saveSmallBlocks(blocks, offset, data, len, index);
  }
  else
  {
    unsigned int index = (pos + len - 1) / io->bbat->blockSize;
    while (index >= blocks.size())
        io->ExtendFile(&blocks);
    unsigned int offset = pos % io->bbat->blockSize;
    unsigned int remainder = len;
    index = pos / io->bbat->blockSize;
    while( remainder > 0 )
    {
      if( index >= blocks.size() ) break;
      unsigned int count = io->bbat->blockSize - offset;
      if ( remainder < count )
          count = remainder;
      io->saveBigBlock( blocks[index], offset, data + totalbytes, count );
      totalbytes += count;
      remainder -= count;
      index++;
      offset = 0;
    }
  }
  if (blocks.size() > 0 && entry->start != blocks[0])
  {
      entry->start = blocks[0];
      io->dirtree->markAsDirty(io->dirtree->indexOf(entry), io->bbat->blockSize);
  }
  m_pos += len;
  return totalbytes;

}

void StreamIO::flush()
{
    io->flush();
}

void StreamIO::updateCache()
{
  // sanity check
  if( !cache_data ) return;

  DirEntry *entry = io->dirtree->entry(entryIdx);
  cache_pos = m_pos - (m_pos % CACHEBUFSIZE);
  unsigned int bytes = CACHEBUFSIZE;
  if( cache_pos + bytes > entry->size ) bytes = entry->size - cache_pos;
  cache_size = read( cache_pos, cache_data, bytes );
}


// =========== Storage ==========

Storage::Storage( const char* filename )
{
  io = new StorageIO( this, filename );
}

Storage::~Storage()
{
  delete io;
}

int Storage::result()
{
  return io->result;
}

bool Storage::open(bool bWriteAccess, bool bCreate)
{
  return io->open(bWriteAccess, bCreate);
}

void Storage::close()
{
  io->close();
}

std::list<std::string> Storage::entries( const std::string& path )
{
  std::list<std::string> result;
  DirTree* dt = io->dirtree;
  DirEntry* e = dt->entry( path, false );
  if( e  && e->dir )
  {
    unsigned parent = dt->indexOf( e );
    std::vector<unsigned> children = dt->children( parent );
    for( unsigned i = 0; i < children.size(); i++ )
      result.push_back( dt->entry( children[i] )->name );
  }
  
  return result;
}

bool Storage::isDirectory( const std::string& name )
{
  DirEntry* e = io->dirtree->entry( name, false );
  return e ? e->dir : false;
}

bool Storage::exists( const std::string& name )
{
    DirEntry* e = io->dirtree->entry( name, false );
    return (e != 0);
}

bool Storage::isWriteable()
{
    return io->writeable;
}

bool Storage::deleteByName( const std::string& name )
{
  return io->deleteByName(name);
}

void Storage::GetStats(unsigned int *pEntries, unsigned int *pUnusedEntries,
      unsigned int *pBigBlocks, unsigned int *pUnusedBigBlocks,
      unsigned int *pSmallBlocks, unsigned int *pUnusedSmallBlocks)
{
    *pEntries = io->dirtree->entryCount();
    *pUnusedEntries = io->dirtree->unusedEntryCount();
    *pBigBlocks = io->bbat->count();
    *pUnusedBigBlocks = io->bbat->unusedCount();
    *pSmallBlocks = io->sbat->count();
    *pUnusedSmallBlocks = io->sbat->unusedCount();
}

// =========== Stream ==========

Stream::Stream( Storage* storage, const std::string& name, bool bCreate, long streamSize )
{
  io = storage->io->streamIO( name, bCreate, (int) streamSize );
}

// FIXME tell parent we're gone
Stream::~Stream()
{
  delete io;
}

std::string Stream::fullName()
{
  return io ? io->fullName : std::string();
}

unsigned long Stream::tell()
{
  return io ? io->tell() : 0;
}

void Stream::seek( unsigned long newpos )
{
  if( io )
      io->seek( (unsigned int) newpos );
}

unsigned long Stream::size()
{
    if (!io)
        return 0;
    DirEntry *entry = io->io->dirtree->entry(io->entryIdx);
    return entry->size;
}

void Stream::setSize(long newSize)
{
    if (!io)
        return;
    if (newSize < 0)
        return;
    if (newSize > UINT_MAX)
        return;
    io->setSize((unsigned int) newSize);
}

int Stream::getch()
{
  return io ? io->getch() : 0;
}

unsigned int Stream::read( unsigned char* data, unsigned int maxlen )
{
  return io ? io->read( data, maxlen ) : 0;
}

unsigned int Stream::write( unsigned char* data, unsigned int len )
{
    return io ? io->write( data, len ) : 0;
}

void Stream::flush()
{
    if (io)
        io->flush();
}

bool Stream::eof()
{
  return io ? io->eof : false;
}

bool Stream::fail()
{
  return io ? io->fail : true;
}
