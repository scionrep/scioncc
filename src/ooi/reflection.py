
import sys, os, contextlib, urllib2

class EggCache(object):
    def __init__(self,cache_dir,repo=None):
        self.cache_dir = cache_dir
        self.repo = repo
    def get_object(self, class_name, module_name, egg_name=None, egg_repo=None, args=[], kwargs={}):
        class_object = self.get_class(class_name, module_name, egg_name, egg_repo)
        return class_object(*args,**kwargs)
    def get_class(self, class_name, module_name, egg_name=None, egg_repo=None):
        if not egg_name:
            module_object = __import__(module_name, fromlist=[class_name])
            return getattr(module_object, class_name)
        self.add_egg(egg_name, egg_repo or self.repo)
        return self.get_class(class_name, module_name)
    def add_egg(self, name, repo):
        full_path = self.cache_dir + '/' + name
        if full_path in sys.path: return
        self.get_egg(name,repo)
        sys.path.append(full_path)
    def get_egg(self, name, repo=None):
        repo = repo or self.repo
        full_path = self.cache_dir + '/' + name
        if not os.path.exists(full_path):
            full_url = repo + '/' + name
            with open(full_path, 'wb') as output:
                with contextlib.closing(urllib2.urlopen(full_url)) as input:
                    output.write(input.read())
        return full_path