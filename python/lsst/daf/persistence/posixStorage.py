#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import copy
import cPickle
import importlib
import inspect
import os

import yaml

from lsst.daf.persistence import LogicalLocation, Persistence, Policy, StorageList, Registry, CfgHelper
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
from .safeFileIo import SafeFilename

class PosixStorage(object):

    @staticmethod
    def makeFromCfg(cfg):
        if isinstance(cfg['storage'], str):
            # todo import the string
            raise NotImplemented("Need to handle importing storage from string")
        if inspect.isclass(cfg['storage']):
            initArgs = CfgHelper.getFuncArgs(cfg['storage'].__init__, cfg)
            cfg['storage'] = cfg['storage'](**initArgs)
        return cfg['storage']


    def __init__(self, root=None):
        """Initializer for PosixStorage

        Parameters
        ----------
        root : string, optional
               Path to the root of the repository. If none, assumes path will be determined.
        """
        self.log = pexLog.Log(pexLog.Log.getDefaultLog(), "daf.persistence.butler")
        self.root = root
        if self.root and not os.path.exists(self.root):
            os.makedirs(self.root)

        # Always use an empty Persistence policy until we can get rid of it
        persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)

        self.registry = Registry.create(location=self.root)

    def __repr__(self):
        return 'PosixStorage(root=%s)' % self.root

    @staticmethod
    def getMapperClass(root):
        """Returns the mapper class associated with a repository root.

        Supports the legacy _parent symlink search (which was only ever posix-only. This should not be used by
        new code and repositories; they should use the Repository parentCfg mechanism."""
        if not (root):
            return None

        # Find a "_mapper" file containing the mapper class name
        basePath = root
        mapperFile = "_mapper"
        globals = {}
        while not os.path.exists(os.path.join(basePath, mapperFile)):
            # Break abstraction by following _parent links from CameraMapper
            if os.path.exists(os.path.join(basePath, "_parent")):
                basePath = os.path.join(basePath, "_parent")
            else:
                raise RuntimeError(
                        "No mapper provided and no %s available" %
                        (mapperFile,))
        mapperFile = os.path.join(basePath, mapperFile)

        # Read the name of the mapper class and instantiate it
        with open(mapperFile, "r") as f:
            mapperName = f.readline().strip()
        components = mapperName.split(".")
        if len(components) <= 1:
            raise RuntimeError("Unqualified mapper name %s in %s" %
                    (mapperName, mapperFile))
        pkg = importlib.import_module(".".join(components[:-1]))
        return getattr(pkg, components[-1])

    def mapperClass(self):
        """Get the class object for the mapper specified in the stored repository"""
        if self.root is None:
            raise RuntimeError("Root must be specified before PosixRegistry can be used")
        return PosixStorage.getMapperClass(self.root)

    def setCfg(self, repoCfg):
        """Writes the configuration to root in the repository on disk.

        :param repoCfg: the Policy cfg to be written
        :return: None
        """
        if self.root is None:
            raise RuntimeError("Root must be specified before PosixRegistry can be used")
        path = os.path.join(self.root, 'repoCfg.yaml')
        repoCfg.dumpToFile(path)

    def loadCfg(self):
        """Reads the configuration from the repository on disk at root.

        :return: the Policy cfg
        """
        if self.root is None:
            raise RuntimeError("Root must be specified before PosixRegistry can be used")
        path = os.path.join(self.root, 'repoCfg.yaml')
        return Policy(filePath=path)

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        :param butlerLocation: the location & formatting for the object to be written.
        :param obj: the object to be written.
        :return: None
        """
        self.log.log(pexLog.Log.DEBUG, "Put location=%s obj=%s" % (butlerLocation, obj))

        additionalData = butlerLocation.getAdditionalData()
        storageName = butlerLocation.getStorageName()
        locations = butlerLocation.getLocations()

        pythonType = butlerLocation.getPythonType()
        if pythonType is not None and isinstance(pythonType, basestring):
            pythonType = CfgHelper.getModule(pythonType)

        # todo this effectively defines the butler posix "do serialize" command to be named "put". This has
        # implications; write now I'm worried that any python type that can be written to disk and has a method
        # called 'put' will be called here (even if it's e.g. destined for FitsStorage). We might want a somewhat
        # more specific API.
        if hasattr(pythonType, 'butlerWrite'):
            pythonType.butlerWrite(obj, butlerLocation=butlerLocation)
            return

        with SafeFilename(locations[0]) as locationString:
            logLoc = LogicalLocation(locationString, additionalData)

            if storageName == "PickleStorage":
                with open(logLoc.locString(), "wb") as outfile:
                    cPickle.dump(obj, outfile, cPickle.HIGHEST_PROTOCOL)
                return

            if storageName == "ConfigStorage":
                obj.save(logLoc.locString())
                return

            if storageName == "FitsCatalogStorage":
                flags = additionalData.getInt("flags", 0)
                obj.writeFits(logLoc.locString(), flags=flags)
                return

            # Create a list of Storages for the item.
            storageList = StorageList()
            storage = self.persistence.getPersistStorage(storageName, logLoc)
            storageList.append(storage)

            if storageName == 'FitsStorage':
                self.persistence.persist(obj, storageList, additionalData)
                return

            # Persist the item.
            if hasattr(obj, '__deref__'):
                # We have a smart pointer, so dereference it.
                self.persistence.persist(obj.__deref__(), storageList, additionalData)
            else:
                self.persistence.persist(obj, storageList, additionalData)

    def read(self, butlerLocation):
        """Read from a butlerLocation.

        :param butlerLocation:
        :return: a list of objects as described by the butler location. One item for each location in
                 butlerLocation.getLocations()
        """
        additionalData = butlerLocation.getAdditionalData()
        # Create a list of Storages for the item.
        storageName = butlerLocation.getStorageName()
        results = []
        locations = butlerLocation.getLocations()
        pythonType = butlerLocation.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                # import this pythonType dynamically
                pythonTypeTokenList = pythonType.split('.')
                importClassString = pythonTypeTokenList.pop()
                importClassString = importClassString.strip()
                importPackage = ".".join(pythonTypeTokenList)
                importType = __import__(importPackage, globals(), locals(), [importClassString], -1)
                pythonType = getattr(importType, importClassString)

        # see note re. discomfort with the name 'butlerWrite' in the write method, above. Same applies to butlerRead.
        if hasattr(pythonType, 'butlerRead'):
            results = pythonType.butlerRead(butlerLocation=butlerLocation)
            return results

        for locationString in locations:
            logLoc = LogicalLocation(locationString, additionalData)

            if storageName == "PafStorage":
                finalItem = pexPolicy.Policy.createPolicy(logLoc.locString())
            elif storageName == "YamlStorage":
                finalItem = Policy(filePath=logLoc.locString())
            elif storageName == "PickleStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError, "No such pickle file: " + logLoc.locString()
                with open(logLoc.locString(), "rb") as infile:
                    finalItem = cPickle.load(infile)
            elif storageName == "FitsCatalogStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError, "No such FITS catalog file: " + logLoc.locString()
                hdu = additionalData.getInt("hdu", 0)
                flags = additionalData.getInt("flags", 0)
                finalItem = pythonType.readFits(logLoc.locString(), hdu, flags)
            elif storageName == "ConfigStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError, "No such config file: " + logLoc.locString()
                finalItem = pythonType()
                finalItem.load(logLoc.locString())
            else:
                storageList = StorageList()
                storage = self.persistence.getRetrieveStorage(storageName, logLoc)
                storageList.append(storage)
                itemData = self.persistence.unsafeRetrieve(
                        butlerLocation.getCppType(), storageList, additionalData)
                finalItem = pythonType.swigConvert(itemData)
            results.append(finalItem)

        return results

    def exists(self, location):
        """Check if 'location' exists relative to root.

        :param location:
        :return:
        """
        if self.root is None:
            raise RuntimeError("Root must be specified before PosixRegistry can be used")
        return os.path.exists(os.path.join(self.root, location))

    def locationWithRoot(self, location):
        """Get the full path to the location.

        :param location:
        :return:
        """
        if self.root is None:
            raise RuntimeError("Root must be specified before PosixRegistry can be used")
        return os.path.join(self.root, location)

    def lookup(self, *args, **kwargs):
        """Perform a lookup in the registry"""
        return self.registry.lookup(*args, **kwargs)