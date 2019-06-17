package uk.ac.wellcome.storage.locking.memory

import java.util.UUID

import uk.ac.wellcome.storage.locking.LockDaoTestCases

class MemoryLockDaoTest extends LockDaoTestCases[String, UUID, Unit] with MemoryLockDaoFixtures
