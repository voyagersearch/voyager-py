import os
import logging
import sys
import subprocess
import shutil
import glob
import platform


class VgBuildPyWorker:
    """
    Copy the python worker into position.
    """

    def __init__(self, cwd, plat, builddir, bv):
        self.cwd = cwd
        self.plat = plat
        bvars = bv.buildvars()
        self.xdeps = bv.depsdir(bvars, plat, builddir)
        self.pydir = os.path.join(bv.buildroot(builddir), "py")
        logging.info("deps = %s" % self.xdeps)
        logging.info("python_bindir = %s" % self.pydir)


    def build(self):
        """All we do is copy the py file into output area."""
        dstdir = self.pydir
        target = os.path.join(dstdir, "VoyagerWorkerPy.py")

        if not os.path.exists(dstdir):
            os.mkdir(dstdir)
        elif os.path.exists(target):
            os.remove(target)

        shutil.copyfile(os.path.join(self.cwd, "VoyagerWorkerPy.py"), target)

        # And we need the vgextractors stuff.
        target = os.path.join(self.pydir, "vgextractors")
        if not os.path.exists(target):
            os.makedirs(target)
        for ext in ["py", "md"]:
            for fin in glob.glob("%s/*.%s" % (os.path.join(self.cwd, "vgextractors"), ext)):
                shutil.copy(fin, target)


if __name__ == "__main__":
    if sys.platform == 'darwin':
        plat="darwin_x86_64"
    elif sys.platform == 'win32':
        # On windos always put output into 32 bit area.
        plat="win32_x86"
    else:
        raise Exception("unknown platform: %s" % sys.platform)

    sys.path.append(os.path.join("..", "common"))
    import vgbuildvars

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    # Location of the master build scripts:
    builddir = os.path.join(os.getcwd(), "..", "build", plat)

    builder = VgBuildPyWorker(os.getcwd(), plat, builddir, vgbuildvars)
    builder.build()
