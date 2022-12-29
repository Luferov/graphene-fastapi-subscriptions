import inspect
from graphene import ObjectType, Field


def subscription_bind(cls):
    """Auto bind resolve function for subscription.

    In graphene > 3.0 the subscribtions must hase subscribe_{field_name} resolver.
    This decorator bind Field.resolver to subscribe_{field_name}.
    """
    if not issubclass(cls, ObjectType):
        return cls

    for field_name, field_type in list(vars(cls).items()):
        if (
                isinstance(field_type, Field)
                and not hasattr(cls, f'subscribe_{field_name}')
                and hasattr(field_type, 'resolver')
                and inspect.isasyncgenfunction(field_type.resolver)
        ):
            setattr(cls, f'subscribe_{field_name}', field_type.resolver)
    return cls
