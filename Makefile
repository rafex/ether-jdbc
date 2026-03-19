# Set the directory for this project so make deploy need not receive PROJECT_DIR
PROJECT_DIR         := ether-jdbc
PROJECT_GROUP_ID    := dev.rafex.ether.jdbc
PROJECT_ARTIFACT_ID := ether-jdbc
DEPENDENCY_COORDS   := ether-database-core.version=dev.rafex.ether.database:ether-database-core

# Include shared build logic
include ../build-helpers/common.mk
include ../build-helpers/git.mk
