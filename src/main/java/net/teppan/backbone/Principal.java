package net.teppan.backbone;

import java.util.Set;

/**
 * An authenticated user identity carried through the backbone runtime.
 *
 * <p>Instances are immutable. The {@code roles} set is always unmodifiable.
 *
 * <pre>{@code
 * Principal user = new Principal("u-42", "Alice", Set.of("ADMIN", "USER"));
 * if (user.hasRole("ADMIN")) { ... }
 * }</pre>
 *
 * @param id          unique identifier for this principal
 * @param displayName human-readable name (for logging and UI)
 * @param roles       the set of roles granted to this principal
 */
public record Principal(String id, String displayName, Set<String> roles) {

    /** Canonical compact constructor — makes the roles set unmodifiable. */
    public Principal {
        roles = Set.copyOf(roles);
    }

    /**
     * Returns a principal representing an unauthenticated request.
     * Has no roles.
     *
     * @return the anonymous principal
     */
    public static Principal anonymous() {
        return new Principal("anonymous", "Anonymous", Set.of());
    }

    /**
     * Returns a principal for internal system tasks (timer jobs, background workers).
     * Has the {@code "SYSTEM"} role.
     *
     * @return the system principal
     */
    public static Principal system() {
        return new Principal("system", "System", Set.of("SYSTEM"));
    }

    /**
     * Returns {@code true} when this principal has the given role.
     *
     * @param role the role to check; never {@code null}
     * @return {@code true} if the role is present
     */
    public boolean hasRole(String role) {
        return roles.contains(role);
    }
}
