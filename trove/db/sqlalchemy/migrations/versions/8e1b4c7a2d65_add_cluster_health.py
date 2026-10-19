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

"""add cluster_health

The health of a cluster as the task manager last found it, one row per
cluster (id: the cluster's), apart from clusters: a cluster saved from
an older read must not write back an older health.

Revision ID: 8e1b4c7a2d65
Revises: 5d2f8a4c1e93
Create Date: 2026-10-19 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8e1b4c7a2d65'
down_revision: Union[str, None] = '5d2f8a4c1e93'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'cluster_health',
        sa.Column('id', sa.String(36), primary_key=True, nullable=False),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('detail', sa.Text(), nullable=True),
        sa.Column('since', sa.DateTime(), nullable=True),
        sa.Column('created', sa.DateTime(), nullable=True),
        sa.Column('updated', sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table('cluster_health')
