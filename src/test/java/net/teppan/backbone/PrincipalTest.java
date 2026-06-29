package net.teppan.backbone;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrincipalTest {

    @Test
    void hasRole_returnsTrueForGrantedRole() {
        var p = new Principal("u1", "Alice", Set.of("ADMIN", "USER"));
        assertThat(p.hasRole("ADMIN")).isTrue();
        assertThat(p.hasRole("USER")).isTrue();
    }

    @Test
    void hasRole_returnsFalseForMissingRole() {
        var p = new Principal("u1", "Alice", Set.of("USER"));
        assertThat(p.hasRole("ADMIN")).isFalse();
    }

    @Test
    void anonymous_hasNoRoles() {
        var p = Principal.anonymous();
        assertThat(p.id()).isEqualTo("anonymous");
        assertThat(p.roles()).isEmpty();
        assertThat(p.hasRole("USER")).isFalse();
    }

    @Test
    void system_hasSystemRole() {
        var p = Principal.system();
        assertThat(p.id()).isEqualTo("system");
        assertThat(p.hasRole("SYSTEM")).isTrue();
    }

    @Test
    void roles_setIsUnmodifiable() {
        var p = new Principal("u1", "Alice", Set.of("USER"));
        assertThatThrownBy(() -> p.roles().add("ADMIN"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rolesSetPassedToConstructor_isDefensiveCopied() {
        var mutable = new java.util.HashSet<String>();
        mutable.add("USER");
        var p = new Principal("u1", "Alice", mutable);
        mutable.add("ADMIN");
        assertThat(p.roles()).containsOnly("USER");
    }
}
