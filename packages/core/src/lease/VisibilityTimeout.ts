export const VisibilityTimeout = {
  calculateLeaseExpiration(leaseDurationMs: number): Date {
    return new Date(Date.now() + leaseDurationMs);
  },

  isLeaseExpired(leaseUntil: Date | null): boolean {
    if (!leaseUntil) {
      return true;
    }

    return Date.now() >= leaseUntil.getTime();
  },

  getRemainingTime(leaseUntil: Date | null): number {
    if (!leaseUntil) {
      return 0;
    }

    return Math.max(0, leaseUntil.getTime() - Date.now());
  },
};