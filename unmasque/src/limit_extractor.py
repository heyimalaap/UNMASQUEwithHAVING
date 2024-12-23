from .context import UnmasqueContext
from loguru import logger
import itertools
import string
import datetime
from typing import Literal
from abc import ABC, abstractmethod
from copy import deepcopy

# ==== HELPER CLASSES ====
class AbstractValueGen(ABC):
    """
    The base class for a value generator.
    
    A value generator creates a new unique value for a perticular data-type. 
    The `next()` method gives you a new value and the `reset()` method resets
    the state of the value generator.
    
    Note that the `next()` method can return a `None` in case new values cannot
    be generated.
    """

    @abstractmethod
    def next(self):
        """
        Returns a new value if it can be generated. Otherwise returns a `None`.
        """
        pass

    @abstractmethod
    def reset(self):
        """
        Resets the state of the value generator.
        """
        pass
        
class NumericValueGen(AbstractValueGen):
    def __init__(self, lower = 0, upper = None, precision = 1):
        self.lower = lower
        self.upper = upper
        self.precision = precision
        self.next_value = lower
        
    def next(self):
        ret = self.next_value
        if self.upper and ret > self.upper:
            return None
        self.next_value += self.precision
        return ret
        
    def reset(self):
        self.next_value = self.lower
    
class StringValueGen(AbstractValueGen):
    def __init__(self, length=10, charset=string.ascii_lowercase):
        self.charset = charset
        self.length = length
        self.iter = itertools.product(charset, repeat=length)
    
    def next(self):
        try:
            return "".join(self.iter.__next__())
        except:
            return None

    def reset(self):
        self.iter = itertools.product(self.charset, length=self.length)
        
class DateValueGen(AbstractValueGen):
    def __init__(self, lower = datetime.date(2000, 1, 1), upper = None):
        self.lower = lower
        self.upper = upper
        self.next_value = lower
        
    def next(self):
        ret = self.next_value
        if self.upper and ret > self.upper:
            return None
        self.next_value += datetime.timedelta(days=1)
        return ret

    def reset(self):
        self.next_value = self.lower

class ConstantValueGen(AbstractValueGen):
    def __init__(self, constant):
        self.constant = constant
        self.ret = constant
    
    def next(self):
        ret = self.ret
        self.ret = None
        return ret
    
    def reset(self):
        self.ret = self.constant
        
        
GEN_TYPE = Literal['Numeric', 'String', 'Constant', 'Date']

class RowGenerator:
    def __init__(self):
        self.attribs = []
        self.generator_list = []
        self.current_row = []

    def push_generator(self, attrib_name: str, attrib_type: GEN_TYPE, **args):
        # Add the attribute name to the list
        self.attribs.append(attrib_name)
        
        # Add the corresponding generator to the generator list
        if attrib_type == 'Numeric':
            self.generator_list.append(NumericValueGen(**args))
        elif attrib_type == 'String':
            self.generator_list.append(StringValueGen(**args))
        elif attrib_type == 'Date':
            self.generator_list.append(DateValueGen(**args))
        elif attrib_type == 'Constant':
            self.generator_list.append(ConstantValueGen(**args))
            
        # Generate a value and store it in the row cache
        self.current_row.append(self.generator_list[-1].next())
            
    def generate_row(self):
        """
        Generates a single row. If there is no more generate, returns `None`
        """
        # If current_row in None, then we don't have more rows to generate
        if self.current_row is None:
            return None

        ret = deepcopy(self.current_row)
        for i, gen in enumerate(self.generator_list):
            next_val = gen.next()
            if next_val is None:
                # If the last generator overflows, then we have exausted all the
                # generators. Thus we should return None
                if i == len(self.generator_list) - 1:
                    self.current_row = None
                    continue
                gen.reset()
                self.current_row[i] = gen.next()
                continue
            self.current_row[i] = next_val
            break
        
        return ret
        
    def reset(self):
        self.current_row = []
        for generator in self.generator_list:
            generator.reset()                           # Reset the generator
            self.current_row.append(generator.next())   # Repopulate the row cache

# ==== EXTRACTOR FUNC ====
def limit_extractor(ctx: UnmasqueContext):
    logger.info('Starting Join extractor')

    logger.info('Finished Join extractor')