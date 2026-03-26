install(
    TARGETS boruvka_spla_exe
    RUNTIME COMPONENT boruvka_spla_Runtime
)

if(PROJECT_IS_TOP_LEVEL)
  include(CPack)
endif()
