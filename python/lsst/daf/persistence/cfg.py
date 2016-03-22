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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import inspect

class CfgHelper(object):

    defaultArgsToIgnore = ('self', 'cls')

    @staticmethod
    def getFuncArgs(func, cfg, ignoredKeys=defaultArgsToIgnore):
        """Gets keys from cfg that match the arguments needed for func.

        Will raise if arguments without default values are not represented by keys in cfg.

        Parameters
        ----------
        func : function object
               This object will be inspected to determine required & optional input argument names.

        cfg : map
              Will look for keys in map that match the arguments in func.

        ignoredKeys : list of string
                      Argument names in func that should be ignored. The obvious use case is when func is a member function,
                      since it is generally expected that a member function will be called similar to ``class.func(...)``.
                      However, there may be other use cases.

        Returns
        -------
        map
            Map where keys match input argument names for func. Values come from cfg.
        """
        ret = {}
        argSpec = inspect.getargspec(func)
        if argSpec.defaults is not None:
            requiredArgs = argSpec.args[:len(argSpec.args) - len(argSpec.defaults)]
        else:
            requiredArgs = argSpec.args
        for arg in argSpec.args:
            if arg in ignoredKeys:
                continue
            if arg in cfg:
                ret[arg] = cfg[arg]
            elif arg in requiredArgs:
                raise RuntimeError("Missing argument:%s from cfg, can't get all arguments to call %s" % (arg, func))
        return ret

    @staticmethod
    def getModule(pythonType):
        if not isinstance(pythonType, basestring):
            raise RuntimeError("get_module requires a string argument, got:%s" % pythonType)
        # import this pythonType dynamically
        pythonTypeTokenList = pythonType.split('.')
        importClassString = pythonTypeTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(pythonTypeTokenList)
        importType = __import__(importPackage, globals(), locals(), [importClassString], -1)
        pythonType = getattr(importType, importClassString)
        return pythonType
