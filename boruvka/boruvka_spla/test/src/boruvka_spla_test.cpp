#include "lib.hpp"

auto main() -> int
{
  auto const lib = library {};

  return lib.name == "boruvka_spla" ? 0 : 1;
}
