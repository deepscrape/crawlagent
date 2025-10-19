import inspect
import typing
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Type, Union, cast

import crawl4ai as _c4


class ConfigValidator(ABC):
    """Abstract base class for configuration validators."""

    def __init__(self, config_class, config_name):
        self.config_class = config_class
        self.config_name = config_name
    
    @abstractmethod
    def validate_specific(self, instance: Any) -> None:
        """Hook for subclasses to add specific validation logic."""
        pass

    def validate(self, config: Dict) -> Any:
        """Validate the config and return the validated object."""
        # First, validate the structure of the JSON
        if not isinstance(config, dict):
            raise ValueError(f"{self.config_name} must be a dictionary")
            
        if "type" not in config:
            raise ValueError(f"Missing 'type' field in {self.config_name} configuration")
        
        if config["type"] != self.config_name:
            raise ValueError(f"Invalid type: {config['type']}. Expected '{self.config_name}'")
            
        if "params" not in config:
            raise ValueError(f"Missing 'params' field in {self.config_name} configuration")
        
        # Create a config instance
        try:
            instance = self.config_class.load(config)
            if not isinstance(instance, self.config_class):
                raise ValueError(f"Invalid {self.config_name} type")
                
            # Validate all attributes based on signature
            sig = inspect.signature(self.config_class.__init__)
            param_types = {
                param_name: param.annotation 
                for param_name, param in sig.parameters.items() 
                if param_name != 'self' and param.annotation != inspect.Parameter.empty
            }
            
            # Check each attribute against expected type
            for attr_name, expected_type in param_types.items():
                attr_value = getattr(instance, attr_name)
                
                # Skip None values which are typically allowed for any type
                if attr_value is None:
                    continue
                
                # Handle Union types
                if hasattr(expected_type, "__origin__") and expected_type.__origin__ is Union:
                    type_valid = any(self._is_instance_safe(attr_value, t) for t in expected_type.__args__)
                else:
                    type_valid = self._is_instance_safe(attr_value, expected_type)
                
                if not type_valid:
                    actual_type = type(attr_value).__name__
                    expected_type_name = getattr(expected_type, "__name__", str(expected_type))
                    raise TypeError(f"Attribute '{attr_name}' has invalid type: got {actual_type}, expected {expected_type_name}")
            
            self.validate_specific(instance)

            return instance
            
        except Exception as e:
            # Enhance the error message
            if "has invalid type" in str(e):
                raise e
            raise ValueError(f"Invalid {self.config_name.lower()} configuration: {str(e)}") from e
        
    @staticmethod
    def _is_instance_safe(obj: Any, type_hint: Any) -> bool:
        """Safely check if object is instance of type, handling generics and enums."""
        # First check for Enum types
        if isinstance(type_hint, type) and issubclass(type_hint, Enum):
            # If it's an enum and obj is a string, check if the string matches any enum value
            if isinstance(obj, str):
                try:
                    # Get all possible values from the enum and normalize for comparison
                    valid_values = [str(e.value).lower() for e in type_hint]
                    # Check if the lowercase string matches any enum value
                    return obj.lower() in valid_values
                except Exception:
                    return False
            # If obj is already the correct enum type
            return isinstance(obj, type_hint)
        
        # Handle basic types with strict checking
        if type_hint is str:
            return isinstance(obj, str)
        elif type_hint is int:
            return isinstance(obj, int) and not isinstance(obj, bool)  # Prevent bool being accepted as int
        elif type_hint is float:
            return isinstance(obj, (float, int)) and not isinstance(obj, bool)  # Allow int for float, but not bool
        elif type_hint is bool:
            return isinstance(obj, bool) or obj in (0, 1)  # Only allow bool or 0/1
        elif type_hint in (dict, list):
            return isinstance(obj, type_hint)
        
        # Handle List, Dict specifically - most common cases
        if hasattr(type_hint, "__origin__"):
            if type_hint.__origin__ is list or type_hint.__origin__ is typing.List:
                return isinstance(obj, list)
            elif type_hint.__origin__ is dict or type_hint.__origin__ is typing.Dict:
                return isinstance(obj, dict)
        
        # For any other type, just try a direct isinstance
        try:
            return isinstance(obj, type_hint)
        except TypeError:
            # If isinstance fails with TypeError, fall back to checking against the base class
            return isinstance(obj, type_hint.__origin__) if hasattr(type_hint, "__origin__") else False

# Alternative factory function approach
def create_validator(config_class, config_name):
    # Create a concrete class that implements the abstract method
    class ConcreteValidator(ConfigValidator):
        def validate_specific(self, instance: Any) -> None:
            # Default implementation does nothing
            pass
    return ConcreteValidator(config_class, config_name)
    
# This would let you do:
BrowserConfigValidator = create_validator(_c4.BrowserConfig, "BrowserConfig")
CrawlerRunConfigValidator = create_validator(_c4.CrawlerRunConfig, "CrawlerRunConfig")
SeedingConfigValidator = create_validator(_c4.SeedingConfig, "SeedingConfig")