#ifndef SJTU_UNORDERED_MAP_H
#define SJTU_UNORDERED_MAP_H

// #include <functional>

#include "container/stl/map.h"

namespace sjtu {

template <class Key, class T, class Compare = std::less<Key>>
class unordered_map : public map<Key, T, Compare> {};  // NOLINT

}  // namespace sjtu

#endif