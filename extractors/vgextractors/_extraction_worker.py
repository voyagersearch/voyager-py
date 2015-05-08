import os
import json
from _worker import Base
from _router import Router
from _vgdexfield import VgDexField
import _error
import zmq


class ExtractionWorker(Base):
    """Abstract worker for performing simple extractions.

    Subclasses must implement extract method.
    """


    def __init__(self, job_factory):
        Base.__init__(self, job_factory)
        self._nexthop_sock = None
        self._nexthop_addr = None


    def extract(self, infile, xjob, extractor):
        """ Perform extraction, subclass must implement.

        xjob - The ExtractionJob or subclass (this is expected to be updated with
        the results of extraction).

        extractor - The extractor name.
        """
        raise _error.IllegalStateException('ExtractionWorker.extract must be implemented')


    def accept(self, extractor):
        """Check if extractor name given is supported.

        Subclass should override to restrict the extractor names they
        can handle. Default accepts any.
        """
        return True



    def run_job(self, job):
        runok = True
        router = Router(job, self.get_vpid())
        ename = job.get('extractor')
        if (ename is None) or len(ename) == 0:
            self.log.error("ERROR no extractor name specified")
            raise _error.JobSpecError('no extractor specified')
        if self.accept(ename):
            router.start("EXTRACT")
            infile = self.resolve_path(job)
            if infile is None:
                job.error(_error.VgErr.RESOLUTION_FAIL, "resolution failed")
                runok = False
            else:
                self.extract(infile, job, ename)
        else:
            self.log.error("ERROR unknown extractor type: %s" % ename)
            raise _error.JobSpecError('unknown extractor')

        if router.has_current():
            # If we have current stage, we require a next stage.
            nextstage = router.complete()
            if nextstage is None:
                raise _error.RouteException("no next stage")
            if nextstage.name == "NOP":
                print job.to_json(5)
            elif not nextstage.is_routable():
                raise _error.RouteException("bad address for next stage")
            else:
                self.forward_result(job, nextstage)
        else:
            print job.to_json(5)
        return runok



    def forward_result(self, job, stage):
        """ Forward over zmq.

        job   - Current job
        stage - Stage to forward to.
        """
        # If we are already connected to next hop over zmq, reuse.
        self.log.info("forwarding to %s at %s" % (stage.name, stage.address))
        if not self._nexthop_sock is None:
            if not self._nexthop_addr == stage.address:
                self._nexthop_sock.close()
                self._nexthop_sock = None
        if self._nexthop_sock is None:
            self._nexthop_sock = self.context.socket(zmq.PUSH)
            self._nexthop_sock.connect(stage.address)
            self._nexthop_addr = stage.address
        self._nexthop_sock.send(job.to_json())


    def resolve_path(self, job):
        """Return path to the input file."""
        resolved = None
        path = job.get('file')
        if (path is None) or len(path) == 0:
            try:
                path = self.get_local_file(job)
            except _error.IOException, e:
                self.log.error("get_local_file failed %s" % e.msg)
                job.error_trace("get_local_file failed %s" % e.msg)
            if path is None:
                self.log.error("path empty and resolution failed for %s" % job.get('file'))
                job.error_trace("path empty and resolution failed for %s" % job.get('file'))
        if not path is None:
            self.status.send_status("reading %s" % path)
            if (os.path.exists(path)):
                resolved = path
            else:
                self.log.error("path not found %s" % path)
                job.error_trace("path not found %s" % path)
        return resolved


    def get_local_file(self, job):
        """Try to ask for a file in case where job has no file property.

        @throws error.IOException
        """
        msg = {}
        if job.has(VgDexField.PATH):
            msg['path'] = job.get(VgDexField.PATH)
        if job.has(VgDexField.LOCATION):
            msg['location'] = job.get(VgDexField.LOCATION)

        entry = job.get('entry', {})
        if entry.has_key(VgDexField.COMPONENT_FILE):
            cf = entry[VgDexField.COMPONENT_FILE]
            if not cf is None:
                # Component file is expected to be a list of strings.
                msg['parts'] = cf

        return self.exec_command("GET_LOCAL_FILE", json.dumps(msg))
