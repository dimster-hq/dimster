//! Shell script builder used by `--generate-script` mode across cloud CLIs.
//!
//! Keeps bash formatting (headers, section dividers, heredocs) uniform so
//! generated setup and teardown scripts are readable and easy to audit.

/// Accumulates shell script lines for `--generate-script` output.
pub struct Script {
    lines: Vec<String>,
}

impl Script {
    pub fn new() -> Self {
        Self { lines: Vec::new() }
    }

    pub fn comment(&mut self, text: &str) {
        self.lines.push(format!("# {}", text));
    }

    pub fn blank(&mut self) {
        self.lines.push(String::new());
    }

    pub fn section(&mut self, title: &str) {
        self.blank();
        self.lines.push(format!("# {}", "=".repeat(60)));
        self.lines.push(format!("# {}", title));
        self.lines.push(format!("# {}", "=".repeat(60)));
        self.blank();
    }

    pub fn cmd(&mut self, command: &str) {
        self.lines.push(command.to_string());
    }

    /// Emit a literal (single-quoted delimiter) heredoc — no shell expansion.
    pub fn heredoc(&mut self, command: &str, content: &str) {
        self.heredoc_with_tag(command, "EOF", true, content);
    }

    /// Emit a heredoc with an explicit delimiter tag and optional shell expansion.
    pub fn heredoc_with_tag(&mut self, command: &str, tag: &str, literal: bool, content: &str) {
        if literal {
            self.lines.push(format!("{} <<'{}'", command, tag));
        } else {
            self.lines.push(format!("{} <<{}", command, tag));
        }
        for line in content.lines() {
            self.lines.push(line.to_string());
        }
        self.lines.push(tag.to_string());
    }

    pub fn render(&self) -> String {
        let mut out = self.lines.join("\n");
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out
    }
}

impl Default for Script {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_sections_and_heredocs() {
        let mut s = Script::new();
        s.cmd("#!/usr/bin/env bash");
        s.comment("demo");
        s.section("Step 1");
        s.heredoc("kubectl apply -f -", "kind: ConfigMap\nmetadata:\n  name: a");
        let rendered = s.render();
        assert!(rendered.starts_with("#!/usr/bin/env bash\n# demo\n"));
        assert!(rendered.contains("# Step 1\n"));
        assert!(rendered.contains("kubectl apply -f - <<'EOF'\nkind: ConfigMap"));
        assert!(rendered.ends_with("EOF\n"));
    }

    #[test]
    fn expanding_heredoc_uses_unquoted_delimiter() {
        let mut s = Script::new();
        s.heredoc_with_tag("cat > out.yaml", "CONFIG_EOF", false, "image: ${REPO}/dimster");
        let rendered = s.render();
        assert!(rendered.contains("cat > out.yaml <<CONFIG_EOF\nimage: ${REPO}/dimster\nCONFIG_EOF\n"));
    }
}
