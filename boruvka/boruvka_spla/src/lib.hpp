#pragma once

#include <spla.hpp>

#include <string>

/** Placeholder for Borůvka MST on SPLA; initializes SPLA (CPU when accelerator is None). */
struct library {
    library();
    ~library();

    std::string name;

private:
    spla::Library spla_;
};
