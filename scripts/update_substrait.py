# Requires protoc 3.19.04
# https://github.com/protocolbuffers/protobuf/releases/tag/v3.19.4

import os
import shutil

from pathlib import Path


# ------------------------------
# Functions

# NOTE: this is a function in case more sophisticated cleanup is desirable in the future
def CleanupDir(dirpath):
    """ Deletes the directory :dirpath:. """
    shutil.rmtree(dirpath)


def PrepareRepo(repo_dirpath, repo_url, repo_tag):
    """ Clones the repository to a temporary directory to be cleaned up afterwards. """

    # Clone Proto Files on a specific git tag
    os.system(f'git clone --branch {repo_tag} -- {repo_url} {repo_dirpath}')


def GenerateProtobufCode(proto_rootdir, src_rootdir, proto_fpath):
    """ Command that compiles :proto_fpath: and outputs to :src_rootdir:. """

    os.system(f'protoc -I={proto_rootdir} --cpp_out={src_rootdir} {proto_fpath}')


def GenerateSubstraitCode(thirdparty_rootdir):
    # Define reference variables
    proj_name = 'substrait'
    pkg_name  = 'substrait'
    repo_url  = 'https://github.com/substrait-io/substrait'
    repo_tag  = 'v0.53.0'

    # e.g. .../third_party/substrait
    proj_dirpath = thirdparty_rootdir / proj_name
    repo_dirpath = thirdparty_rootdir / proj_name / f'git-{pkg_name}'
    pkg_dirpath  = thirdparty_rootdir / proj_name / pkg_name

    CleanupDir(pkg_dirpath)
    PrepareRepo(repo_dirpath, repo_url, repo_tag)

    # Create output directory for generated code
    os.makedirs(pkg_dirpath / 'extensions')

    # Create directory structure for source files
    proto_rootdir = repo_dirpath  / 'proto'

    # Generate code for each protobuf definition file
    for proto_dpath, _, proto_fnames in os.walk(proto_rootdir / 'substrait'):
        for proto_fname in proto_fnames:
            proto_fpath = os.path.join(proto_dpath, proto_fname)
            GenerateProtobufCode(proto_rootdir, proj_dirpath, proto_fpath)

    CleanupDir(repo_dirpath)


def GenerateMohairCode(thirdparty_rootdir):
    # Define reference variables
    proj_name = 'mohair'
    pkg_name  = 'mohair'
    repo_url  = 'https://github.com/drin/mohair-protocol'
    repo_tag  = 'feat-skytether-engine'

    # e.g. .../third_party/substrait
    proj_dirpath = thirdparty_rootdir / proj_name
    repo_dirpath = thirdparty_rootdir / proj_name / f'git-{pkg_name}'
    pkg_dirpath  = thirdparty_rootdir / proj_name / pkg_name

    CleanupDir(pkg_dirpath)
    PrepareRepo(repo_dirpath, repo_url, repo_tag)

    # Create output directory for generated code
    os.makedirs(pkg_dirpath)

    # Create directory structure for source files
    proto_rootdir = repo_dirpath  / 'proto'

    # Generate code for each protobuf definition file
    for proto_dpath, _, proto_fnames in os.walk(proto_rootdir / 'mohair'):
        for proto_fname in proto_fnames:
            proto_fpath = os.path.join(proto_dpath, proto_fname)
            GenerateProtobufCode(proto_rootdir, proj_dirpath, proto_fpath)

    CleanupDir(repo_dirpath)


# ------------------------------
# Main Logic

if __name__ == '__main__':
    script_fpath    = Path(__file__).resolve()
    root_dirpath    = script_fpath.parent.parent
    project_dirpath = root_dirpath / 'third_party'

    # /usr/local/bin/protoc
    protoc_version = os.popen('protoc --version').read()
    print(f'Protoc version: {protoc_version}')

    GenerateSubstraitCode(project_dirpath)
    GenerateMohairCode(project_dirpath)
