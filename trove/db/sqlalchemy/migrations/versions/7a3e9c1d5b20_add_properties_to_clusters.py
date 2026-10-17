# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""add properties to clusters

What a cluster was created with that the datastore keeps for its life,
as JSON (the storage mode of a Vertica cluster and where its communal
storage is); NULL is none.

Revision ID: 7a3e9c1d5b20
Revises: e513612d4200
Create Date: 2026-10-17 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a3e9c1d5b20'
down_revision: Union[str, None] = 'e513612d4200'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('clusters', sa.Column('properties', sa.Text(),
                                        nullable=True))


def downgrade() -> None:
    op.drop_column('clusters', 'properties')
