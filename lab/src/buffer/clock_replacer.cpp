//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  clock_hand = 0;
  replace_size = 0;
  num_frames = num_pages;
  frame_pin.resize(num_frames);
  frame_refs.resize(num_frames);
  for (size_t i = 0; i < num_frames; ++i) {
    frame_pin[i] = true;
    frame_refs[i] = false;
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (replace_size == 0) {
    latch_.unlock();
    return false;
  }
  while (true) {
    if (!frame_pin[clock_hand]) {
      if (frame_refs[clock_hand]) {
        frame_refs[clock_hand] = false;
        clock_hand = (clock_hand + 1) % num_frames;
      } else {
        *frame_id = clock_hand;
        frame_pin[clock_hand] = true;
        replace_size--;
        latch_.unlock();
        return true;
      }
    } else {
      clock_hand = (clock_hand + 1) % num_frames;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  if (!frame_pin[frame_id]) {
    frame_pin[frame_id] = true;
    replace_size--;
  }
  latch_.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if (frame_pin[frame_id]) {
    frame_pin[frame_id] = false;
    replace_size++;
  }
  frame_refs[frame_id] = true;
  latch_.unlock();
}

size_t ClockReplacer::Size() {
  return replace_size;
}

}  // namespace bustub
