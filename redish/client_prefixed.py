from redis.exceptions import ResponseError
from redish.client import Client
from redish.utils import maybe_list


class ClientPrefixed(Client):

    prefix = None

    def __init__(self, host=None, port=None, db=None, prefix=None,
                 serializer=None, **kwargs):
        self.prefix = prefix + "_"
        super(ClientPrefixed, self).__init__(host=None, port=None, db=None,
                                             serializer=None, **kwargs)


    def update(self, mapping):
        """Update database with the key/values from a :class:`dict`."""
        return self.api.mset(dict((self.prefix + key, self.prepare_value(value))
                                for key, value in mapping.items()))

    def rename(self, old_name, new_name):
        """Rename key to a new name."""
        try:
            self.api.rename(self._mkey_prefixed(old_name), self._mkey_prefixed(new_name))
        except ResponseError, exc:
            if "no such key" in exc.args:
                raise KeyError(old_name)
            raise

    def keys(self, pattern="*"):
        """Get a list of all the keys in the database, or
        matching ``pattern``."""
        pattern = self.prefix + pattern
        return super(ClientPrefixed, self).keys(pattern)

    def __getitem__(self, name):
        """``x.__getitem__(name) <==> x[name]``"""
        name = self._mkey_prefixed(name)
        value = self.api.get(name)
        if value is None:
            raise KeyError(name)
        return self.value_to_python(value)

    def __setitem__(self, name, value):
        """``x.__setitem(name, value) <==> x[name] = value``"""
        return self.api.set(self._mkey_prefixed(name), self.prepare_value(value))

    def __delitem__(self, name):
        """``x.__delitem__(name) <==> del(x[name])``"""
        name = self._mkey_prefixed(name)
        if not self.api.delete(name):
            raise KeyError(name)

    def __contains__(self, name):
        """``x.__contains__(name) <==> name in x``"""
        return self.api.exists(self._mkey_prefixed(name))

    def _mkey_prefixed(self, names):
        mkey = [self.prefix + key for key in maybe_list(names)]
        return ":".join(mkey)
