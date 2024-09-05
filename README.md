**Pole
portable library for structured storage**

POLE is a portable C++ library to access structured storage. It is designed to be compatible with Microsoft structured storage, also sometimes known as OLE Compound Document.

A structured storage is normally used to store files inside another file; you can even have complex directory tree inside. It can be treated as a filesystem inside a file. The most popular use of structured storage is in Microsoft Office. Structured storage does not offer compression nor encryption support, hence usually it is used only for interoperability purposes.

Compared to structured storage routines offered by Microsoft Windows API, POLE has the advantage that it is portable. You can access any structured storage in the supported platform, you don't need to rely on Windows library.

This library is initially developed by Ariya Hidayat <ariya@kde.org>. Since currently it's not updated for a long time, i decided to host my changes right here.

Download
Use Github repository for contributing to the project, accessing the source code online, filing bug reports and checking what's new.

**How to use**
To compile POLE, you need a C++ compiler, with standard C++ library which supports STL (Standard Template Library).

POLE is just a library, you have to integrate it as part of your application/project. Since it consists of one source file and one header file, it does not make much sense to compile POLE as separate library (either static or dynamic). Simply add these files to your project and you are done.

The source tarball of POLE demonstrates the use of POLE to build poledump, a small utility to extract any stream inside a structured storage. If you use g++ as the compiler, you can compile poledump using the following command:

g++ -o poledump pole.cpp poledump.cpp
You may use poledump like the example below:

poledump budget.xls Workbook result.xls
The above command will extract a stream named Workbook inside Excel document (budget.xls) and save it as result.xls. File result.xls will still be recognized as a valid workbook (in raw format). Launch Microsoft Excel, open this file, and convince yourself.

To compile POLEView, you need Qt (from Trolltech) version 4.x. Most Linux distributions normally already package the latest version of Qt, so it is likely that you need to worry about this. The build command for poleview is:

qmake && make
**Platforms**
POLE has been succesfully tested under the following platforms/compilers:

Microsoft Windows: Microsoft Visual C++ 2008, Borland C++ Compiler 5.5.1, OpenWatcom 1.3, Microsoft Visual C++, Digital Mars C/C++ 8.41n
Linux: GCC 4.x (including 4.3), GCC 3.x (may also work with 2.9.x), Intel C++ compiler
If you can compile and use POLE in another platform/compiler, please kindly send a report.

**Links**
POLE is being proposed to be included in the next release of Boost. The interface for Boost has been written by Israel Cabrera (israel@segurmatica.com) and Jorge Lodos Vigil (lodos@segurmatica.com). See polepp project for details.

There are also other projects which have the same goal as POLE.

Libgsf (from Jody Golberg), written in plain C (with glib dependency), is also able to read and write structured storage. If you're in Linux/Unix, please use libgsf instead of POLE because libgsf has more features, well developed, and mature already.

Should you want to access structured storage with Java, Jakarta POI is your best choice. If you're interested in the internals of structured storage, you can refer to its excellent documentation.

There is also libole2, but I believe this is already obsoleted by libgsf.

WINE project has its own implementation of structured storage.

Few projects still use cole from Roberto Arturo Tena Sanchez. You can check cole from xlhtml project.

Laola, a small Perl utility, is among the first project which tries to reverse engineer the format of structured storage.

**Copyright and License**
POLE Copyright © 2002-2005 Ariya Hidayat <ariya@kde.org>. All rights reserved.

Copyright © 2009 Dmitry Fedorov, Center for Bio-Image Informatics. All rights reserved.

Copyright © 2010 Michel Boudinot. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of the authors nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
