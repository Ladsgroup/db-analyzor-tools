class Column():
    def __init__(self, type_, size_, not_null, unsigned, auto_increment):
        self.type_ = type_
        self.size_ = size_
        self.not_null = not_null
        self.unsigned = unsigned
        self.auto_increment = auto_increment

    @classmethod
    def newFromAbstractSchema(cls, schema):
        type_ = schema['type'].replace(
            'string', 'binary').replace('integer', 'int')
        size_ = schema['options'].get('length', 0)

        if type_ == 'binary' and not schema['options'].get(
                'fixed'):
            type_ = 'varbinary'
        elif type_ in ('blob', 'text'):
            if size_ < 256:
                type_ = 'tinyblob'
            elif size_ < 65536:
                type_ = 'blob'
            elif size_ < 16777216:
                type_ = 'mediumblob'
        elif type_ == 'mwtinyint':
            type_ = 'tinyint'
        elif type_ == 'mwenum':
            type_ = 'enum'
            size_ = set([
                i.lower() for i in schema['options'].get(
                    'CustomSchemaOptions', {}).get(
                    'enum_values', [])])
        elif type_ == 'mwtimestamp':
            if schema['options'].get(
                    'CustomSchemaOptions', {}).get(
                    'allowInfinite', False):
                type_ = 'varbinary'
            else:
                type_ = 'binary'
            size_ = 14
        elif type_ in ('datetimetz'):
            type_ = 'timestamp'
        not_null = schema['options'].get('notnull', False)
        unsigned = schema['options'].get('unsigned', False)
        auto_increment = schema['options'].get(
            'autoincrement', False)

        return cls(type_, size_, not_null, unsigned, auto_increment)
