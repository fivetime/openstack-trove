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

"""add message_partition to instances

The guest message partition (the RabbitMQ broker of the guest agent) an
instance was placed on; NULL is the default partition.

Revision ID: e513612d4200
Revises: 95418fe2a1c8
Create Date: 2026-10-15 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import String


# revision identifiers, used by Alembic.
revision: str = 'e513612d4200'
down_revision: Union[str, None] = '95418fe2a1c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'instances',
        sa.Column(
            'message_partition',
            String(64),
            nullable=True
        )
    )
    op.create_index('instances_message_partition_idx', 'instances',
                    ['message_partition', 'deleted'])


def downgrade() -> None:
    op.drop_index('instances_message_partition_idx', table_name='instances')
    op.drop_column('instances', 'message_partition')
