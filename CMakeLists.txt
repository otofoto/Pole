project(POLE)

set(VERSION "0.5")

add_library(POLE STATIC pole/pole.h pole/pole.cpp)
target_include_directories(POLE PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/pole)
