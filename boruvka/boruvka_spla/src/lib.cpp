#include "lib.hpp"

library::library() : name{"boruvka_spla"}, spla_{} {
    static_cast<void>(spla_.set_accelerator(spla::AcceleratorType::None));
}

library::~library() { spla_.finalize(); }
