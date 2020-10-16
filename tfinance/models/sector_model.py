from .meta import Base
from sqlalchemy import Column, String


class SectorModel(Base):
    __tablename__ = 'sectors'

    code = Column(String, primary_key=True)
    name = Column(String)

    def __repr__(self):
        fmt = '<SectorModel(code="{}", name="{}")>'
        return fmt.format(self.code, self.name)
